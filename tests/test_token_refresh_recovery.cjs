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

function accountHarness(apiRequest) {
    const result = harness(apiRequest);
    const healthStart = html.indexOf('        function getAccountHealthDisplay(');
    const healthEnd = html.indexOf('        function setAccountHealthProgress(', healthStart);
    vm.runInContext(html.slice(healthStart, healthEnd), result.context);
    const refreshStart = html.indexOf('        async function refreshSingleAccount(');
    const refreshEnd = html.indexOf('        // 通用导出下载', refreshStart);
    vm.runInContext(html.slice(refreshStart, refreshEnd), result.context);
    return result;
}

test('manual review overrides OAuth success in status and account badge', () => {
    const {context} = accountHarness();
    const record = {status: 'manual_review', manual_review_required: true, refresh_status: 'success'};
    assert.equal(context.getTokenRefreshStatus(record), 'review');
    assert.match(context.getAccountHealthDisplay(record).label, /失效.*人工检查/);
    assert.equal(context.getTokenRefreshStatus({status: 'refresh_error', refresh_status: 'failed'}), 'failed');
});

test('snapshot counts paused separately and displays pause instead of success', async () => {
    const {context, nodes} = harness(async url => {
        if (url === '/accounts/health-check') return {running: false, results: {}};
        return {accounts: [{email_id: 'paused@example.com', status: 'manual_review',
            manual_review_required: true, manual_review_reason: 'security interrupt',
            health_summary: '失效邮箱，待人工检查', health_checked_at: '2026-11-09T00:00:01',
            refresh_status: 'success', refresh_sequence: 3, last_refresh_at: '2026-11-09T00:00:00'}]};
    });
    await context.loadTokenRefreshSnapshot();
    assert.equal(nodes.get('refreshStatReview').textContent, '1');
    assert.equal(nodes.get('refreshStatSuccess').textContent, '0');
    assert.match(nodes.get('tokenRefreshResultList').innerHTML, /已暂停刷新/);
    nodes.get('tokenRefreshResultFilter').value = 'review';
    context.renderTokenRefreshResults();
    context.renderTokenRefreshRetryBtn();
    assert.match(nodes.get('tokenRefreshResultList').innerHTML, /paused@example.com/);
    assert.equal(nodes.get('tokenRefreshRetryBtn').disabled, true);
});

test('bulk retry excludes paused accounts and does not request manual override', async () => {
    const attempts = [];
    const {context} = harness(async (url, options) => {
        if (url === '/accounts/refresh-one') {
            const payload = JSON.parse(options.body);
            attempts.push(payload);
            return {record: {status: 'healthy', refresh_status: 'success'}};
        }
        if (url === '/accounts/health-check') return {running: false, results: {}};
        return {accounts: []};
    });
    context.window.__tokenRefreshAccounts = [{email: 'paused@example.com', refreshStatus: 'review'},
        {email: 'failed@example.com', refreshStatus: 'failed'}];
    await context.tokenRefreshRetryFailed();
    assert.deepEqual(attempts, [{email: 'failed@example.com'}]);
});

test('review filter cannot accidentally retry other failed accounts', async () => {
    const {context, nodes, notices} = harness(async () => {throw new Error('must not issue request');});
    context.window.__tokenRefreshAccounts = [{email: 'failed@example.com', refreshStatus: 'failed'}];
    nodes.get('tokenRefreshResultFilter').value = 'review';
    await context.tokenRefreshRetryFailed();
    assert.ok(notices.some(([text]) => text.includes('单独复检')));
});

test('manual account recheck sends explicit override and reports pause truthfully', async () => {
    const requests = [];
    const {context, notices} = accountHarness(async (url, options) => {
        requests.push([url, JSON.parse(options.body)]);
        return {record: {status: 'manual_review', refresh_status: 'success', manual_review_required: true,
            summary: '失效邮箱，待人工检查'}};
    });
    await context.refreshSingleAccount('paused@example.com');
    assert.deepEqual(requests, [['/accounts/refresh-one', {email:'paused@example.com', recheck:true}]]);
    assert.ok(notices.some(([text, kind]) => kind === 'warning' && text.includes('已暂停')));
    assert.ok(!notices.some(([, kind]) => kind === 'success'));
});

test('successful manual recovery and OAuth status use consistent notification', async () => {
    const {context, notices} = accountHarness(async () => ({record: {
        status: 'healthy', refresh_status:'success', manual_review_required:false}}));
    await context.refreshSingleAccount('recovered@example.com');
    assert.ok(notices.some(([text, kind]) => kind === 'success' && text.includes('刷新成功')));
});

test('old paused snapshot cannot override newer manual recovery', () => {
    const {context} = harness();
    const merged = context.mergeTokenRefreshAccount({email:'one@example.com', status:'healthy',
        refresh_status:'success', manual_review_required:false, refresh_sequence:10}, {
        status:'manual_review', refresh_status:'failed', manual_review_required:true, refresh_sequence:9,
        checked_at:'2026-11-09T10:00:00'});
    assert.equal(merged.refreshStatus, 'success');
});

test('batch log reports skipped manual-review accounts', () => {
    const {context, nodes} = harness();
    context.renderTokenRefreshLog({started_at:'2026-11-09T00:00:00', completed_at:'2026-11-09T00:01:00',
        running:false, total:4, checked:4, skipped:2});
    assert.match(nodes.get('tokenRefreshLogSummary').textContent, /已跳过 2 个待人工检查账号/);
});
