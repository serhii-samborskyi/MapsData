
async function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function processCampaign(campaignId, campaignName) {
  const API_BASE = 'https://fac10661-ee9f-4358-b944-f137d7d73a1a-00-30y5jt13c33qm.riker.replit.dev';
  let currentTabId = null;

  try {
    while (true) {
      // Get next request
      const requestResponse = await fetch(`${API_BASE}/api/campaign/${campaignName}/requests`);
      if (!requestResponse.ok) {
        if (requestResponse.status === 404) {
          // No more requests, mark campaign as completed
          await fetch(`${API_BASE}/api/campaign/${campaignId}/complete`, {
            method: 'POST'
          });
          return { success: true };
        }
        throw new Error('Failed to get requests');
      }

      const requestData = await requestResponse.json();
      const request = requestData.requests[0];

      // Mark request as in use
      await fetch(`${API_BASE}/api/request/${request.id}/status`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: `status=inuse`
      });

      // Open Maps search in new tab
      const tab = await chrome.tabs.create({
        url: `https://www.google.com/maps/search/${encodeURIComponent(request.req_text)}`
      });
      currentTabId = tab.id;

      // Wait for scraping results
      const results = await new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(new Error('Scraping timed out'));
        }, 60000);

        chrome.runtime.onMessage.addListener(function listener(message, sender) {
          if (sender.tab.id === currentTabId) {
            if (message.action === 'scrapedData') {
              clearTimeout(timeout);
              chrome.runtime.onMessage.removeListener(listener);
              resolve(message.data);
            } else if (message.action === 'scrapeError') {
              clearTimeout(timeout);
              chrome.runtime.onMessage.removeListener(listener);
              reject(new Error(message.error));
            }
          }
        });
      });

      // Save results
      for (const contact of results) {
        await fetch(`${API_BASE}/api/contacts`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
          body: new URLSearchParams({
            campaign_id: campaignId,
            request_id: request.id,
            ...contact
          })
        });
      }

      // Mark request as completed
      await fetch(`${API_BASE}/api/request/${request.id}/status`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: `status=completed`
      });

      // Close tab and wait before next request
      if (currentTabId) {
        await chrome.tabs.remove(currentTabId);
        currentTabId = null;
      }
      await delay(2000);
    }
  } catch (error) {
    if (currentTabId) {
      await chrome.tabs.remove(currentTabId);
    }
    throw error;
  }
}

chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
  if (request.action === 'startCampaign') {
    processCampaign(request.campaignId, request.campaignName)
      .then(() => sendResponse({ success: true }))
      .catch(error => sendResponse({ error: error.message }));
    return true;
  }
});
