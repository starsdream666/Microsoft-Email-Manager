const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const html = fs.readFileSync(path.join(__dirname, '../static/index.html'), 'utf8');
const start = html.indexOf('// ==================== Token 刷新管理页');
const end = html.indexOf('// ==================== /Token 刷新管理页');
const source = html.slice(start, end);

function harness(apiRequest) {
    const nodes = new Map();
    const notices = [];
    const context = vm.createContext({
        console,
        document: {getElementById(id) {
            if (!nodes.has(id)) nodes.set(id, {value: '', textContent: '', innerHTML: '', style: {}, disabled: false});
            return nodes.get(id);
        }},
        window: {setTimeout: () => 1, clearTimeout: () => {}},
        apiRequest,
        escapeHtml: value => String(value),
        showNotification: (...args) => notices.push(args),
    });
    vm.runInContext(source, context);
    context.document.getElementById('tokenRefreshResultFilter').value = 'all';
    return {context, nodes, notices};
}

test('stopping retry prevents scheduling the next account', async () => {
    let releaseFirst;
    let signalStarted;
    const started = new Promise(resolve => {signalStarted = resolve;});
    const attempts = [];
    const {context} = harness(async (url, opts) => {
        if (url === '/accounts/refresh-one') {
            const email = JSON.parse(opts.body).email;
            attempts.push(email);
            if (attempts.length === 1) {
                signalStarted();
                await new Promise(resolve => {releaseFirst = resolve;});
            }
            return {record: {status: 'healthy', summary: 'OK', checked_at: '2026-11-09T00:00:00'}};
        }
        if (url === '/accounts/health-check') return {running: false, results: {}};
        if (url.startsWith('/accounts?page=')) return {accounts: []};
        throw new Error(url);
    });
    context.window.__tokenRefreshAccounts = [
        {email: 'first@example.com', refreshStatus: 'failed'},
        {email: 'second@example.com', refreshStatus: 'failed'},
    ];
    const running = context.tokenRefreshRetryFailed();
    await started;
    await context.tokenRefreshStop();
    releaseFirst();
    await running;
    assert.deepEqual(attempts, ['first@example.com'], 'stop handler continued refreshing remaining accounts');
});


test('stop button is available while running and hidden after completion', () => {
    const {context, nodes} = harness();
    assert.doesNotMatch(html.match(/<button[^>]+id="tokenRefreshStopBtn"[^>]*>/)[0], /class="[^"]*\bhidden\b/);
    context.setTokenRefreshRunningUI(true);
    assert.equal(nodes.get('tokenRefreshStopBtn').style.display, '');
    assert.equal(nodes.get('tokenRefreshStopBtn').disabled, false);
    context.setTokenRefreshRunningUI(false);
    assert.equal(nodes.get('tokenRefreshStopBtn').style.display, 'none');
});

test('backend stop submits task id and polls partial completion', async () => {
    const requests = [];
    const state = {task_id: 'run-one', running: false, stopped: true, checked: 1, total: 2,
        started_at: '2026-11-09T01:00:00', completed_at: '2026-11-09T01:01:00', results: {}};
    const {context, nodes} = harness(async (url, options) => {
        requests.push([url, options]);
        if (url === '/accounts/health-check/stop') return {...state, running: true, stop_requested: true};
        if (url === '/accounts/health-check') return state;
        if (url.startsWith('/accounts?page=')) return {accounts: [{email_id:'one@example.com'}, {email_id:'two@example.com'}]};
        throw new Error(url);
    });
    context.window.__tokenRefreshState = {task_id: 'run-one', running: true};
    await context.tokenRefreshStop();
    assert.equal(JSON.parse(requests[0][1].body).task_id, 'run-one');
    assert.equal(requests[0][0], '/accounts/health-check/stop');
    assert.equal(nodes.get('tokenRefreshProgressText').textContent, '1/2');
    assert.equal(nodes.get('tokenRefreshProgressBar').style.width, '50%');
    assert.match(nodes.get('tokenRefreshLogSummary').textContent, /已停止.*1\/2/);
});

test('failed stop leaves running controls usable and does not claim success', async () => {
    const {context, nodes, notices} = harness(async url => {
        if (url === '/accounts/health-check/stop') throw new Error('permission denied');
        if (url === '/accounts/health-check') return {task_id: 'run-one', running: true, results: {}};
        return {accounts: []};
    });
    context.window.__tokenRefreshState = {task_id: 'run-one', running: true};
    await context.tokenRefreshStop();
    assert.equal(nodes.get('tokenRefreshStopBtn').disabled, false);
    assert.ok(notices.some(([text, kind]) => kind === 'error' && text.includes('permission denied')));
    assert.ok(!notices.some(([text]) => text.includes('已请求停止')));
});

test('concurrent stop clicks submit only one backend request', async () => {
    let release;
    let stopCalls = 0;
    const {context} = harness(async url => {
        if (url === '/accounts/health-check/stop') {
            stopCalls++;
            return await new Promise(resolve => {release = resolve;});
        }
        if (url === '/accounts/health-check') return {task_id: 'run-one', running: true, stop_requested: true, results: {}};
        return {accounts: []};
    });
    context.window.__tokenRefreshState = {task_id: 'run-one', running: true};
    const first = context.tokenRefreshStop();
    await context.tokenRefreshStop();
    assert.equal(stopCalls, 1);
    release({task_id: 'run-one', running: true, stop_requested: true});
    await first;
});

test('newer operation wins even when older health probe completed later', () => {
    const {context} = harness();
    const merged = context.mergeTokenRefreshAccount({email: 'one@example.com', refresh_sequence: 20,
        status: 'healthy', health_summary: 'new-success', health_checked_at: '2026-11-09T00:00:02',
        refresh_status: 'success', last_refresh_at: '2026-11-09T00:00:01',
        last_refresh_success_at: '2026-11-09T00:00:02'}, {
        refresh_sequence: 19, status: 'auth_error', summary: 'old-failure', checked_at: '2026-11-09T00:00:03',
        refresh_status: 'failed', last_refresh_at: '2026-11-09T00:00:00', last_refresh_success_at: '2026-11-08T00:00:00'});
    assert.equal(merged.healthSummary, 'new-success');
    assert.equal(merged.refreshStatus, 'success');
    assert.equal(merged.lastRefreshSuccessAt, '2026-11-09T00:00:02');
});

test('snapshot passes operation order to merge instead of completion time', async () => {
    const {context, nodes} = harness(async url => {
        if (url === '/accounts/health-check') return {running: false, results: {'one@example.com': {
            refresh_sequence: 7, status: 'auth_error', summary: 'old-failure', checked_at: '2026-11-09T00:00:03',
            refresh_status: 'failed', last_refresh_at: '2026-11-09T00:00:00'}}};
        return {accounts: [{email_id: 'one@example.com', refresh_sequence: 8, status: 'healthy',
            health_summary: 'current-success', health_checked_at: '2026-11-09T00:00:02', refresh_status:'success',
            last_refresh_at: '2026-11-09T00:00:01'}]};
    });
    await context.loadTokenRefreshSnapshot();
    assert.equal(nodes.get('refreshStatSuccess').textContent, '1');
    assert.equal(context.window.__tokenRefreshAccounts[0].healthSummary, 'current-success');
});
