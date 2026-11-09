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

function saved(status, checkedAt) {
    return {email: 'one@example.com', status, health_summary: status, health_checked_at: checkedAt};
}

function result(status, checkedAt) {
    return {status, summary: status, detail: '', checked_at: checkedAt};
}

test('all inline JavaScript remains syntactically valid', () => {
    let count = 0;
    for (const match of html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)) {
        if (/\bsrc\s*=|application\/ld\+json|application\/json/i.test(match[1])) continue;
        new vm.Script(match[2]);
        count++;
    }
    assert.ok(count > 0);
});

test('new saved success wins over old batch failure', () => {
    const {context} = harness();
    const merged = context.mergeTokenRefreshAccount(saved('healthy', '2026-01-02T00:00:00'), result('auth_error', '2026-01-01T00:00:00'));
    assert.equal(merged.refreshStatus, 'success');
    assert.equal(merged.healthSummary, 'healthy');
    assert.equal(merged.healthCheckedAt, '2026-01-02T00:00:00');
});

test('new batch success wins over account response fetched just before completion', () => {
    const {context} = harness();
    const merged = context.mergeTokenRefreshAccount(saved('auth_error', '2026-01-01T00:00:00'), result('healthy', '2026-01-02T00:00:00'));
    assert.equal(merged.refreshStatus, 'success');
    assert.equal(merged.healthCheckedAt, '2026-01-02T00:00:00');
});

test('new failure cannot be masked by older successful batch', () => {
    const {context} = harness();
    assert.equal(context.mergeTokenRefreshAccount(saved('auth_error', '2026-01-02T00:00:00'), result('healthy', '2026-01-01T00:00:00')).refreshStatus, 'failed');
    assert.equal(context.normalizeRefreshStatus('imap_error'), 'failed');
    assert.equal(context.normalizeRefreshStatus('unknown'), 'never');
});

test('retry success leaves failed filter and remains successful after reload of historical batch', async () => {
    const old = result('auth_error', '2026-01-01T00:00:00');
    const newer = result('healthy', '2026-01-02T00:00:00');
    const state = {running: false, checked: 1, total: 1, results: {'one@example.com': old}};
    let retried = false;
    const {context, nodes, notices} = harness(async (url) => {
        if (url === '/accounts/refresh-one') { retried = true; return {record: newer}; }
        if (url === '/accounts/health-check') return state;
        if (url.startsWith('/accounts?page=')) return {total_pages: 1, accounts: [{
            email_id: 'one@example.com', status: retried ? newer.status : old.status,
            health_checked_at: retried ? newer.checked_at : old.checked_at,
        }]};
        throw new Error(url);
    });
    await context.loadTokenRefreshSnapshot();
    assert.equal(nodes.get('refreshStatFailed').textContent, '1');
    nodes.get('tokenRefreshResultFilter').value = 'failed';
    await context.tokenRefreshRetryFailed();
    assert.equal(nodes.get('refreshStatFailed').textContent, '0');
    assert.equal(nodes.get('refreshStatSuccess').textContent, '1');
    assert.match(nodes.get('tokenRefreshResultList').innerHTML, /没有符合条件/);
    assert.ok(notices.some(([text, kind]) => text.includes('成功 1 个，失败 0 个') && kind === 'success'));
    nodes.get('tokenRefreshResultFilter').value = 'success';
    context.renderTokenRefreshResults();
    assert.match(nodes.get('tokenRefreshResultList').innerHTML, /one@example.com/);
});

test('retry counts error records and continues after HTTP failure', async () => {
    const attempted = [];
    const {context, notices} = harness(async (url, opts) => {
        if (url === '/accounts/refresh-one') {
            const email = JSON.parse(opts.body).email;
            attempted.push(email);
            if (email.startsWith('one')) throw new Error('network unavailable');
            return {record: result('auth_error', '2026-01-02T00:00:00')};
        }
        if (url === '/accounts/health-check') return {running: false, results: {}};
        if (url.startsWith('/accounts?page=')) return {accounts: []};
    });
    context.window.__tokenRefreshAccounts = [
        {email: 'one@example.com', refreshStatus: 'failed'},
        {email: 'two@example.com', refreshStatus: 'failed'},
    ];
    await context.tokenRefreshRetryFailed();
    assert.equal(attempted.length, 2);
    assert.ok(notices.some(([text, kind]) => text.includes('成功 0 个，失败 2 个') && kind === 'warning'));
});

test('overlapping snapshots cannot replace a newer completed response', async () => {
    let resolveOld;
    let calls = 0;
    const {context, nodes} = harness(async url => {
        if (url === '/accounts/health-check') return {running: false, results: {}};
        calls++;
        if (calls === 1) return await new Promise(resolve => {resolveOld = resolve;});
        return {accounts: [{email_id: 'one@example.com', status: 'healthy', health_checked_at: '2026-01-02T00:00:00'}]};
    });
    const first = context.loadTokenRefreshSnapshot();
    await context.loadTokenRefreshSnapshot();
    resolveOld({accounts: [{email_id: 'one@example.com', status: 'auth_error', health_checked_at: '2026-01-01T00:00:00'}]});
    await first;
    assert.equal(nodes.get('refreshStatSuccess').textContent, '1');
    assert.equal(nodes.get('refreshStatFailed').textContent, '0');
});
