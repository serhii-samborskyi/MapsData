
function scrapeGoogleMaps() {
  return new Promise((resolve, reject) => {
    let attempts = 0;
    const maxAttempts = 10;
    const pollInterval = setInterval(() => {
      const scrollable = document.querySelector('div[role="list"]');
      if (scrollable || attempts >= maxAttempts) {
        clearInterval(pollInterval);
        if (!scrollable) {
          console.log('Scrollable results pane not found');
          resolve([]);
          return;
        }

        let endOfList = false;
        let lastHeight = 0;
        let noChangeCount = 0;
        const results = new Set();

        const scrollInterval = setInterval(() => {
          scrollable.scrollBy(0, 1000);
          const currentHeight = scrollable.scrollHeight;

          // Check if we've hit the bottom
          if (currentHeight === lastHeight) {
            noChangeCount++;
            if (noChangeCount > 5) {
              endOfList = true;
            }
          } else {
            noChangeCount = 0;
          }
          lastHeight = currentHeight;

          // Collect all place links during scrolling
          const links = document.querySelectorAll('a[href*="/maps/place/"]');
          links.forEach(link => {
            const business = {
              business_name: link.querySelector('div[role="heading"]')?.textContent || '',
              review_count: parseInt(link.querySelector('span[aria-label*="reviews"]')?.textContent.replace(/\D/g, '') || '0'),
              phone: link.closest('[role="listitem"]')?.querySelector('div.fontBodySmall')?.textContent.match(/\(?\d{3}[-\)]?\s?\d{3}[-\s]?\d{4}/) || '',
              domain: link.closest('[role="listitem"]')?.querySelector('a[href*="http"]')?.href || '',
              email: '',
              url: link.href
            };
            if (business.business_name) {
              results.add(JSON.stringify(business));
            }
          });

          if (endOfList) {
            clearInterval(scrollInterval);
            const uniqueResults = Array.from(results).map(r => JSON.parse(r));
            console.log(`Scraped ${uniqueResults.length} unique results`);
            resolve(uniqueResults);
          }
        }, 1000);
      }
      attempts++;
    }, 1000);
  });
}

// Listen for page load
window.addEventListener('load', () => {
  if (window.location.href.includes('/maps/search/')) {
    console.log('Starting scrape for URL:', window.location.href);
    scrapeGoogleMaps()
      .then(data => {
        console.log('Sending scrapedData:', data);
        chrome.runtime.sendMessage({ action: 'scrapedData', data });
      })
      .catch(error => {
        console.log('Sending scrapeError:', error.message);
        chrome.runtime.sendMessage({ action: 'scrapeError', error: error.message });
      });
  }
});
