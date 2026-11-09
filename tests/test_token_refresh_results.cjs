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

test('token success is distinct from mailbox health failure', () => {
    const {context} = harness();
    const merged = context.mergeTokenRefreshAccount({email: 'one@example.com',
        status: 'graph_error', health_summary: 'Graph unavailable', health_checked_at: '2026-11-09T02:00:01',
        refresh_status: 'success', token_rotated: false, refresh_message: '验证成功，Token 未轮换，继续使用原 Token',
        last_refresh_at: '2026-11-09T02:00:00', last_refresh_success_at: '2026-11-09T02:00:00'}, null);
    assert.equal(merged.refreshStatus, 'success');
    assert.equal(merged.tokenRotated, false);
    assert.equal(merged.healthSummary, 'Graph unavailable');
    assert.match(merged.refreshMessage, /未轮换/);
});

test('historical batch cannot overwrite a newer token outcome or timestamps', () => {
    const {context} = harness();
    const merged = context.mergeTokenRefreshAccount({email: 'one@example.com', status: 'auth_error',
        refresh_status: 'failed', token_rotated: false, refresh_message: '保存失败',
        last_refresh_at: '2026-11-09T04:00:00', last_refresh_success_at: '2026-11-09T03:00:00',
        refresh_token_updated_at: '2026-11-09T03:00:00'}, {
        status: 'healthy', refresh_status: 'success', token_rotated: true,
        last_refresh_at: '2026-11-09T01:00:00', last_refresh_success_at: '2026-11-09T01:00:00',
        refresh_token_updated_at: '2026-11-09T01:00:00'});
    assert.equal(merged.refreshStatus, 'failed');
    assert.equal(merged.refreshMessage, '保存失败');
    assert.equal(merged.lastRefreshSuccessAt, '2026-11-09T03:00:00');
    assert.equal(merged.refreshTokenUpdatedAt, '2026-11-09T03:00:00');
});

test('newer batch outcome wins independently of health snapshot order', () => {
    const {context} = harness();
    const merged = context.mergeTokenRefreshAccount({email: 'one@example.com',
        status: 'auth_error', refresh_status: 'failed', last_refresh_at: '2026-11-09T01:00:00'}, {
        status: 'healthy', summary: 'OK', checked_at: '2026-11-09T02:00:01',
        refresh_status: 'success', token_rotated: true, refresh_message: '刷新成功，Token 已更新',
        last_refresh_at: '2026-11-09T02:00:00', refresh_token_updated_at: '2026-11-09T02:00:00'});
    assert.equal(merged.refreshStatus, 'success');
    assert.equal(merged.tokenRotated, true);
    assert.equal(merged.refreshTokenUpdatedAt, '2026-11-09T02:00:00');
});

test('legacy health success does not invent rotation or success timestamps', () => {
    const {context} = harness();
    const merged = context.mergeTokenRefreshAccount({email: 'one@example.com', status: 'healthy',
        health_checked_at: '2026-11-09T02:00:00'}, null);
    assert.equal(merged.refreshStatus, 'success');
    assert.equal(merged.tokenRotated, null);
    assert.equal(merged.lastRefreshSuccessAt, '');
    assert.equal(context.formatTokenRefreshTime('invalid'), '未记录');
    assert.equal(context.formatTokenRefreshTime(null), '未记录');
});

test('snapshot preserves server metadata and renders all three timestamps', async () => {
    const {context, nodes} = harness(async url => {
        if (url === '/accounts/health-check') return {running: false, results: {}};
        return {accounts: [{email_id: 'one@example.com', status: 'imap_error', health_summary: 'IMAP unavailable',
            health_checked_at: '2026-11-09T02:00:01', refresh_status: 'success', token_rotated: true,
            refresh_message: '刷新成功，Token 已更新', last_refresh_at: '2026-11-09T02:00:00',
            last_refresh_success_at: '2026-11-09T02:00:00', refresh_token_updated_at: '2026-11-09T02:00:00'}]};
    });
    await context.loadTokenRefreshSnapshot();
    assert.equal(nodes.get('refreshStatSuccess').textContent, '1');
    assert.equal(nodes.get('refreshStatFailed').textContent, '0');
    const content = nodes.get('tokenRefreshResultList').innerHTML;
    for (const text of ['Token 已更新', '最近尝试', '最近成功', 'Token 更新', 'IMAP unavailable']) {
        assert.ok(content.includes(text));
    }
});

test('single retry counts OAuth success even if mailbox probe fails', async () => {
    const record = {status: 'graph_error', summary: 'Graph unavailable', checked_at: '2026-11-09T02:00:01',
        refresh_status: 'success', token_rotated: false, refresh_message: '验证成功，Token 未轮换，继续使用原 Token',
        last_refresh_at: '2026-11-09T02:00:00'};
    const {context, notices} = harness(async url => {
        if (url === '/accounts/refresh-one') return {record};
        if (url === '/accounts/health-check') return {running: false, results: {}};
        return {accounts: [{email_id: 'one@example.com', ...record, health_summary: record.summary, health_checked_at: record.checked_at}]};
    });
    context.window.__tokenRefreshAccounts = [{email: 'one@example.com', refreshStatus: 'failed'}];
    await context.tokenRefreshRetryFailed();
    assert.equal(context.window.__tokenRefreshAccounts[0].refreshStatus, 'success');
    assert.ok(notices.some(([text, kind]) => text.includes('成功 1 个，失败 0 个') && kind === 'success'));
});
