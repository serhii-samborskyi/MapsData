(() => {
    'use strict';

    const dialog = document.getElementById('mcpConnectionDialog');
    const opener = document.getElementById('mcpConnectionOpen');
    const urlInput = document.getElementById('mcpConnectionUrl');
    const generateButton = document.getElementById('mcpConnectionGenerate');
    const revokeButton = document.getElementById('mcpConnectionRevoke');
    const copyButton = document.getElementById('mcpConnectionCopy');
    const errorBox = document.getElementById('mcpConnectionError');
    const retryButton = document.getElementById('mcpConnectionRetry');
    const status = document.getElementById('mcpConnectionStatus');
    const copyStatus = document.getElementById('mcpConnectionCopyStatus');
    const warning = document.getElementById('mcpConnectionSecretWarning');
    const tabs = Array.from(dialog.querySelectorAll('[data-mcp-tab]'));
    const panels = Object.fromEntries(tabs.map(tab => [tab.dataset.mcpTab, document.getElementById(tab.getAttribute('aria-controls'))]));
    let session = null;

    function isOpen(current) {
        return current && session === current && dialog.open;
    }

    function clearDetails() {
        if (session) session.blocks = null;
        Object.values(panels).forEach(panel => { panel.textContent = ''; });
        window.getSelection()?.removeAllRanges();
        copyButton.disabled = true;
        copyStatus.textContent = 'No connection details.';
        warning.textContent = '';
        warning.hidden = true;
    }

    function selectTab(key, focus = false) {
        if (session) session.tab = key;
        tabs.forEach(tab => {
            const selected = tab.dataset.mcpTab === key;
            tab.setAttribute('aria-selected', String(selected));
            tab.tabIndex = selected ? 0 : -1;
            panels[tab.dataset.mcpTab].hidden = !selected;
            if (selected && focus) tab.focus();
        });
        copyStatus.textContent = session?.blocks ? '' : 'No connection details.';
    }

    function updateControls() {
        const state = session?.state;
        const allowed = state?.can_manage === true && !session.busy;
        urlInput.disabled = !allowed;
        urlInput.readOnly = state?.environment_managed === true;
        urlInput.required = !urlInput.readOnly;
        generateButton.disabled = !allowed || (state.environment_managed === true && state.token_available !== true);
        generateButton.textContent = state?.environment_managed ? 'Copy Connection' : state?.enabled ? 'Replace Token' : 'Generate Token';
        revokeButton.hidden = !state?.managed || !state?.enabled || state?.environment_managed === true;
        revokeButton.disabled = !allowed;
        copyButton.disabled = !allowed || !session?.blocks;
    }

    function showError(message) {
        errorBox.textContent = message;
        errorBox.hidden = !message;
    }

    function connectionState(data, previous = {}) {
        return {
            enabled: data.enabled ?? previous.enabled ?? false,
            managed: data.managed ?? previous.managed ?? false,
            environment_managed: data.environment_managed ?? previous.environment_managed ?? false,
            token_available: data.token_available ?? previous.token_available ?? false,
            can_manage: data.can_manage ?? previous.can_manage ?? false,
            public_url: data.public_url ?? previous.public_url ?? ''
        };
    }

    function defaultUrl() {
        const url = new URL('/mcp/', window.location.origin);
        url.protocol = 'https:';
        return url.href;
    }

    async function request(current, path = '', body) {
        const options = { signal: current.controller.signal, credentials: 'same-origin', cache: 'no-store' };
        if (body !== undefined) {
            options.method = 'POST';
            options.headers = { 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' };
            options.body = JSON.stringify(body);
        }
        const response = await fetch(`/api/mcp/connection${path}`, options);
        const data = await response.json().catch(() => ({}));
        if (!response.ok) {
            if (response.status === 401) throw new Error('Sign in to manage the MCP connection.');
            throw new Error(typeof data.detail === 'string' ? data.detail : data.message || 'The connection request failed.');
        }
        return data;
    }

    function displayState(current) {
        const state = current.state;
        status.textContent = state.can_manage !== true ? 'Login required' : state.environment_managed ? 'Environment managed' : state.enabled ? 'Managed token enabled' : 'Not configured';
        urlInput.value = state.public_url || defaultUrl();
    }

    async function loadConnection(current) {
        if (!isOpen(current)) return;
        current.busy = true;
        current.state = null;
        showError('');
        retryButton.hidden = true;
        status.textContent = 'Loading connection...';
        updateControls();
        try {
            const data = await request(current);
            if (!isOpen(current)) return;
            current.state = connectionState(data);
            displayState(current);
            if (current.state.can_manage !== true) {
                showError(data.message || 'Token management requires LOGIN and PASSWORD to be configured.');
            } else if (data.message) {
                status.textContent += `: ${data.message}`;
            }
        } catch (error) {
            if (isOpen(current)) {
                showError(error.message);
                status.textContent = 'Connection unavailable';
                retryButton.hidden = false;
            }
        } finally {
            if (isOpen(current)) {
                current.busy = false;
                updateControls();
            }
        }
    }

    async function copyDetails(current = session) {
        if (!isOpen(current) || current.busy || !current.blocks || current.state?.can_manage !== true) return;
        const panel = panels[current.tab];
        try {
            await navigator.clipboard.writeText(panel.textContent);
            if (isOpen(current)) copyStatus.textContent = 'Copied.';
        } catch (error) {
            if (!isOpen(current)) return;
            panel.focus();
            const range = document.createRange();
            range.selectNodeContents(panel);
            const selection = window.getSelection();
            selection.removeAllRanges();
            selection.addRange(range);
            copyStatus.textContent = 'Clipboard unavailable. Text selected for manual copying.';
        }
    }

    async function generateConnection(event) {
        event.preventDefault();
        const current = session;
        if (!isOpen(current) || current.busy || current.state?.can_manage !== true) return;
        const environment = current.state.environment_managed === true;
        if (environment && current.state.token_available !== true) return;
        const replacing = !environment && current.state.enabled === true;
        const body = {};
        if (!environment) {
            try {
                const url = new URL(urlInput.value.trim());
                if (url.protocol !== 'https:' || url.username || url.password) throw new Error();
                body.public_url = url.href;
            } catch (error) {
                showError('Enter a valid HTTPS URL without embedded credentials.');
                urlInput.focus();
                return;
            }
            if (replacing) {
                if (!confirm('Replace the existing MCP token? Existing connections will stop working.')) return;
                body.replace_confirmed = true;
            }
        }
        clearDetails();
        showError('');
        current.busy = true;
        status.textContent = environment ? 'Loading connection...' : 'Generating token...';
        updateControls();
        let revealed = false;
        try {
            const data = await request(current, environment ? '/reveal' : '/token', body);
            if (!isOpen(current)) return;
            current.state = connectionState(data, current.state);
            displayState(current);
            if (!Object.keys(panels).every(key => typeof data.blocks?.[key] === 'string' && data.blocks[key].trim())) {
                throw new Error('Connection details were not returned. Reload the connection before trying again.');
            }
            current.blocks = Object.fromEntries(Object.keys(panels).map(key => [key, data.blocks[key]]));
            Object.entries(panels).forEach(([key, panel]) => { panel.textContent = current.blocks[key]; });
            warning.textContent = environment ? 'Contains a secret. Share only with trusted clients.' : 'Contains a secret. This token is shown only once.';
            warning.hidden = false;
            copyStatus.textContent = '';
            revealed = true;
        } catch (error) {
            if (isOpen(current)) {
                await loadConnection(current);
                if (isOpen(current) && current.state?.can_manage === true) showError(error.message);
            }
        } finally {
            if (isOpen(current)) {
                current.busy = false;
                updateControls();
            }
        }
        if (environment && revealed && isOpen(current)) await copyDetails(current);
    }

    async function revokeConnection() {
        const current = session;
        if (!isOpen(current) || current.busy || current.state?.can_manage !== true || !current.state.managed || !current.state.enabled || current.state.environment_managed) return;
        if (!confirm('Revoke the MCP token? Existing connections will stop working.')) return;
        clearDetails();
        showError('');
        current.busy = true;
        status.textContent = 'Revoking token...';
        updateControls();
        try {
            await request(current, '/revoke', { confirmed: true });
            if (isOpen(current)) await loadConnection(current);
        } catch (error) {
            if (isOpen(current)) {
                await loadConnection(current);
                if (isOpen(current) && current.state?.can_manage === true) showError(error.message);
            }
        } finally {
            if (isOpen(current)) {
                current.busy = false;
                updateControls();
            }
        }
    }

    function clearSession(restoreFocus = false) {
        const previous = session;
        clearDetails();
        session = null;
        previous?.controller.abort();
        urlInput.value = '';
        showError('');
        status.textContent = '';
        retryButton.hidden = true;
        selectTab('assistant');
        updateControls();
        if (restoreFocus) (previous?.returnFocus || opener).focus();
    }

    function closeConnection() {
        clearSession();
        dialog.close();
        opener.focus();
    }

    function renderIcons() {
        if (!window.lucide?.createIcons) return;
        try {
            window.lucide.createIcons({ icons: { Copy: window.lucide.Copy, X: window.lucide.X }, attrs: { 'aria-hidden': 'true' } });
            dialog.querySelectorAll('button svg[data-lucide]').forEach(icon => {
                icon.classList.remove('hidden');
                icon.parentElement.querySelector('[data-icon-fallback]').hidden = true;
            });
        } catch (error) {
            // Keep the text labels if the icon library cannot render.
        }
    }

    opener.addEventListener('click', () => {
        if (dialog.open) return;
        session = { controller: new AbortController(), state: null, blocks: null, tab: 'assistant', busy: false, returnFocus: document.activeElement };
        selectTab('assistant');
        dialog.showModal();
        loadConnection(session);
    });
    document.getElementById('mcpConnectionClose').addEventListener('click', closeConnection);
    dialog.addEventListener('cancel', event => { event.preventDefault(); closeConnection(); });
    dialog.addEventListener('close', () => { if (!dialog.open) clearSession(true); });
    window.addEventListener('pagehide', () => { clearSession(); if (dialog.open) dialog.close(); });
    window.addEventListener('beforeunload', () => clearSession());
    document.getElementById('mcpConnectionForm').addEventListener('submit', generateConnection);
    revokeButton.addEventListener('click', revokeConnection);
    copyButton.addEventListener('click', () => copyDetails());
    retryButton.addEventListener('click', () => loadConnection(session));
    tabs.forEach((tab, index) => {
        tab.addEventListener('click', () => selectTab(tab.dataset.mcpTab));
        tab.addEventListener('keydown', event => {
            let next;
            if (event.key === 'ArrowRight') next = (index + 1) % tabs.length;
            else if (event.key === 'ArrowLeft') next = (index + tabs.length - 1) % tabs.length;
            else if (event.key === 'Home') next = 0;
            else if (event.key === 'End') next = tabs.length - 1;
            else return;
            event.preventDefault();
            selectTab(tabs[next].dataset.mcpTab, true);
        });
    });
    document.getElementById('mcpConnectionIcons').addEventListener('load', renderIcons);
    renderIcons();
})();
