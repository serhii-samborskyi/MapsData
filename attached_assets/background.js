async function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchWithRetry(url, options, retries = 3, delayMs = 1000) {
  for (let i = 0; i < retries; i++) {
    try {
      const response = await fetch(url, options);
      if (!response.ok) throw new Error(`HTTP error ${response.status}`);
      return await response.json();
    } catch (error) {
      if (i === retries - 1) throw error;
      console.log(`Retrying fetch (${i + 1}/${retries}) for ${url}: ${error.message}`);
      await delay(delayMs);
    }
  }
}

async function processCampaign(campaignId, campaignName) {
  const baseUrl = 'https://fac10661-ee9f-4358-b944-f137d7d73a1a-00-30y5jt13c33qm.riker.replit.dev/api';
  let currentTabId = null;

  while (true) {
    try {
      // Fetch available requests with retry
      const data = await fetchWithRetry(`${baseUrl}/campaign/${encodeURIComponent(campaignName)}/requests`);
      console.log('API response for requests:', JSON.stringify(data, null, 2));

      if (data.error || !data.requests || data.requests.length === 0) {
        // No more requests, complete the campaign
        await fetch(`${baseUrl}/campaign/${campaignId}/complete`, { method: 'POST' });
        if (currentTabId) {
          await chrome.tabs.remove(currentTabId);
        }
        return { success: true };
      }

      const request = data.requests[0];
      if (!request.req_text || typeof request.req_text !== 'string' || request.req_text.trim() === '') {
        console.log(`Skipping invalid req_text for request ID: ${request.id}`);
        // Mark request as failed
        await fetch(`${baseUrl}/request/${request.id}/status`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ status: 'failed' })
        });
        continue; // Skip to next request
      }

      // Set request status to "inuse"
      await fetch(`${baseUrl}/request/${request.id}/status`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status: 'inuse' })
      });

      // Open Google Maps search page
      const searchUrl = `https://www.google.com/maps/search/${encodeURIComponent(request.req_text)}`;
      const tab = await chrome.tabs.create({ url: searchUrl });
      currentTabId = tab.id;
      console.log(`Opened tab ${currentTabId} for req_text: ${request.req_text}`);

      // Wait for scraping results
      const results = await new Promise((resolve, reject) => {
        const listener = (message, sender, sendResponse) => {
          if (sender.tab.id !== currentTabId) return;
          if (message.action === 'scrapedData') {
            console.log(`Received scrapedData for tab ${currentTabId}:`, message.data);
            chrome.runtime.onMessage.removeListener(listener);
            resolve(message.data);
          } else if (message.action === 'scrapeError') {
            console.log(`Received scrapeError for tab ${currentTabId}:`, message.error);
            chrome.runtime.onMessage.removeListener(listener);
            reject(new Error(message.error));
          }
        };
        chrome.runtime.onMessage.addListener(listener);

        // Timeout after 60 seconds
        setTimeout(() => {
          chrome.runtime.onMessage.removeListener(listener);
          reject(new Error('Scraping timed out for req_text: ' + request.req_text));
        }, 60000);
      });

      // Save scraped data
      console.log(`Saving ${results.length} contacts for request ID: ${request.id}`);
      for (const contact of results) {
        await fetch(`${baseUrl}/contacts`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            campaignId,
            requestId: request.id,
            title: contact.name,
            address: contact.address,
            phone: contact.phone,
            rating: contact.rating,
            reviewsCount: contact.reviews,
            category: contact.category,
            website: contact.website,
            url: contact.url
          })
        });
      }

      // Set request status to "completed"
      await fetch(`${baseUrl}/request/${request.id}/status`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status: 'completed' })
      });

      // Close the tab
      console.log(`Closing tab ${currentTabId}`);
      await chrome.tabs.remove(currentTabId);
      currentTabId = null;

      // Delay before next request
      await delay(2000);
    } catch (error) {
      console.log(`Error in campaign processing: ${error.message}`);
      if (currentTabId) {
        console.log(`Closing tab ${currentTabId} due to error`);
        await chrome.tabs.remove(currentTabId);
        currentTabId = null;
      }
      return { error: 'Error processing campaign: ' + error.message };
    }
  }
}

chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
  const baseUrl = 'https://fac10661-ee9f-4358-b944-f137d7d73a1a-00-30y5jt13c33qm.riker.replit.dev/api';

  if (request.action === 'getActiveCampaigns') {
    fetchWithRetry(`${baseUrl}/campaigns/active`)
      .then(data => {
        if (data.error) {
          sendResponse({ error: data.error });
        } else {
          sendResponse({ campaigns: data.campaigns || [] });
        }
      })
      .catch(error => sendResponse({ error: 'Failed to fetch campaigns: ' + error.message }));
    return true;
  }

  if (request.action === 'startCampaign') {
    processCampaign(request.campaignId, request.campaignName)
      .then(result => sendResponse(result))
      .catch(error => sendResponse({ error: error.message }));
    return true;
  }
});
