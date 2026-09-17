'use strict';
// Exercises the ftp-store state machine against a real Redis — no cameras
// involved. Everything is written under a throwaway prefix and deleted at the
// end, so it is safe to point at the same Redis the service uses.
//
//   node verify-store.js
//
// The prefix is hardcoded to `ftptest` and cannot be overridden from .env —
// cleanup deletes everything under it, so it must never see the real prefix.
//
// Reads REDIS_HOST/PORT/PASSWORD/TLS from .env like the service does.

require('dotenv').config();

// Forced, NOT read from .env. This script deletes everything under its prefix
// during cleanup, so it must never be able to inherit the production prefix or
// the real mirror hash from the environment.
const TEST_PREFIX = 'ftptest';
process.env.REDIS_PREFIX      = TEST_PREFIX;
process.env.REDIS_MIRROR_HASH = TEST_PREFIX + ':mirror';

if (process.env.REDIS_PREFIX !== TEST_PREFIX ||
    !process.env.REDIS_MIRROR_HASH.startsWith(TEST_PREFIX + ':')) {
    console.error('Refusing to run: test prefix was overridden.');
    process.exit(1);
}

const Redis = require('ioredis');
const store = require('./ftp-store');

let pass = 0, fail = 0;
function check(name, cond, detail = '') {
    if (cond) { pass++; console.log(`  ok   ${name}`); }
    else      { fail++; console.log(`  FAIL ${name}${detail ? ' — ' + detail : ''}`); }
}

(async () => {
    const host = process.env.REDIS_HOST;
    const port = parseInt(process.env.REDIS_PORT || '6380');
    const tls  = process.env.REDIS_TLS === 'false' ? undefined : {};

    if (!host) {
        console.error('REDIS_HOST is not set. Add it to .env, or run against a local Redis with:');
        console.error('  REDIS_HOST=127.0.0.1 REDIS_PORT=6379 REDIS_TLS=false node verify-store.js');
        process.exit(1);
    }

    console.log(`Connecting to ${host}:${port} (tls: ${tls ? 'on' : 'off'}, prefix: ${process.env.REDIS_PREFIX})`);

    const redis = new Redis({
        host, port, tls,
        password:       process.env.REDIS_PASSWORD,
        connectTimeout: 10000,
        // Fail fast instead of retrying forever — this is a test, not the service.
        retryStrategy:  times => (times > 2 ? null : 500),
        maxRetriesPerRequest: 2,
    });

    // Without this, ioredis emits an unhandled 'error' event and Node kills the
    // process before the catch below can report anything useful.
    redis.on('error', e => console.error('Redis error:', e.message));

    try {
        await redis.ping();
        console.log('Connected.');
    } catch (e) {
        console.error(`\nCould not reach Redis at ${host}:${port} — ${e.message}`);
        console.error('Check REDIS_HOST / REDIS_PORT / REDIS_PASSWORD / REDIS_TLS in .env,');
        console.error('and that your IP is allowed by the Azure Redis firewall.');
        process.exit(1);
    }

    store.init(redis);

    const phone  = '15760064474';
    const folder = `${phone}_verify_${Date.now()}`;
    const rec    = {
        requestId: 'req-' + Date.now(), phone, ch: 1,
        startTime: '2026-09-17 10:00:00', endTime: '2026-09-17 10:00:20',
        streamType: 1, quality: 'high', alarmFlag: '0', maxAttempts: 3,
    };

    console.log('\n1. Atomic claim');
    check('first createJob wins',      await store.createJob(folder, rec) === true);
    check('second createJob is blocked', await store.createJob(folder, rec) === false);
    let j = await store.getJob(folder);
    check('status is queued', j.status === 'queued', j.status);
    check('in ftp:s:queued', (await store.listState('queued')).includes(folder));

    console.log('\n2. Transitions move state sets');
    await store.transition(folder, 'active', { patch: { attempts: 1 }, reason: 'test', phone });
    check('in ftp:s:active',     (await store.listState('active')).includes(folder));
    check('left ftp:s:queued', !(await store.listState('queued')).includes(folder));
    j = await store.getJob(folder);
    check('attempts is numeric 1', j.attempts === 1, String(j.attempts));

    console.log('\n3. Large numbers survive the round trip');
    const big = 4294967296123;
    await store.transition(folder, 'active', { patch: { fileSize: big }, reason: 'bignum', phone });
    j = await store.getJob(folder);
    check('fileSize exact', j.fileSize === big, `${j.fileSize} !== ${big}`);

    console.log('\n4. Null in a patch clears the field');
    await store.transition(folder, 'active', { patch: { error: 'boom' }, reason: 'err', phone });
    await store.transition(folder, 'active', { patch: { error: null },   reason: 'clear', phone });
    j = await store.getJob(folder);
    check('error removed', j.error === undefined, JSON.stringify(j.error));

    console.log('\n5. Retry schedule');
    await store.transition(folder, 'failed', { patch: { error: 'x' }, reason: 'fail', retryAt: Date.now() - 1000, phone });
    check('appears in dueRetries', (await store.dueRetries()).includes(folder));
    await store.transition(folder, 'queued', { reason: 'retry', phone });
    check('cleared from retry zset', !(await store.dueRetries()).includes(folder));

    console.log('\n6. Indexes');
    check('findByRequestId resolves',  (await store.findByRequestId(rec.requestId))?.folder === folder);
    check('historyByPhone includes it', (await store.historyByPhone(phone)).some(r => r.folder === folder));

    console.log('\n7. Durable queue');
    const f2 = folder + '_b';
    await store.createJob(f2, { ...rec, requestId: 'req2' });
    await store.queuePush(phone, folder);
    await store.queuePush(phone, f2);
    await store.queuePush(phone, folder);   // duplicate push
    check('no duplicates', (await store.queueList(phone)).filter(x => x === folder).length === 1);
    check('order preserved', (await store.queueList(phone)).join() === [f2, folder].join(),
          (await store.queueList(phone)).join());
    const popped = await store.queuePop(phone);
    await store.queueUnshift(phone, popped);
    check('unshift restores head', (await store.queueList(phone))[0] === popped);

    console.log('\n8. Lease');
    check('acquire succeeds',        await store.leaseAcquire(phone, folder, 2000) === true);
    check('second acquire blocked',  await store.leaseAcquire(phone, f2, 2000) === false);
    check('owner is correct',        await store.leaseOwner(phone) === folder);
    check('renew by non-owner fails', await store.leaseRenew(phone, f2, 2000) === false);
    check('renew by owner works',    await store.leaseRenew(phone, folder, 2000) === true);
    await store.leaseRelease(phone, f2);
    check('release by non-owner is a no-op', await store.leaseOwner(phone) === folder);
    await store.leaseRelease(phone, folder);
    check('release by owner clears', await store.leaseOwner(phone) === null);

    console.log('\n9. Legacy mirror shape');
    await store.transition(folder, 'active', { reason: 'mirror-check', phone });
    const mirrored = JSON.parse(await redis.hget(process.env.REDIS_MIRROR_HASH, folder));
    check('mirror status is in_progress', mirrored.status === 'in_progress', mirrored.status);
    check('mirror keeps internalStatus',  mirrored.internalStatus === 'active');
    check('mirror folder matches',        mirrored.folder === folder);
    check('mirror ch is a number',        typeof mirrored.ch === 'number');

    console.log('\n10. Audit log');
    const hist = await store.getLog(folder);
    check('log has entries', hist.length > 0, String(hist.length));
    check('newest first',    hist[0].to === 'active', hist[0]?.to);

    console.log('\n11. Delete clears every index');
    await store.deleteJob(folder);
    await store.deleteJob(f2);
    check('job gone',      await store.getJob(folder) === null);
    check('sets cleared',  (await store.countsByState()).active === 0);
    check('index cleared', await store.findByRequestId(rec.requestId) === null);

    // Clean up anything this run created. Every key is re-checked against the
    // test prefix before deletion — belt and braces around a destructive call.
    const safe = k => typeof k === 'string' && k.startsWith(TEST_PREFIX + ':');
    const targets = [
        store.K.queue(phone), store.K.active(phone), store.K.idxPhone(phone),
        store.K.retry(), store.K.idxReq(), process.env.REDIS_MIRROR_HASH,
        ...await redis.keys(TEST_PREFIX + ':*'),
    ].filter(safe);
    if (targets.length) await redis.del(...[...new Set(targets)]);

    console.log(`\n${pass} passed, ${fail} failed`);
    await redis.quit();
    process.exit(fail ? 1 : 0);
})().catch(e => {
    console.error('\nverify-store crashed:', e && e.stack ? e.stack : e);
    process.exit(1);
});

process.on('unhandledRejection', e => {
    console.error('\nUnhandled rejection:', e && e.stack ? e.stack : e);
    process.exit(1);
});
