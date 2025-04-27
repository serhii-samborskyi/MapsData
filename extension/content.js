
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
        const results = [];

        const scrollInterval = setInterval(() => {
          scrollable.scrollBy(0, 5000);
          const currentHeight = scrollable.scrollHeight;

          if (currentHeight === lastHeight) {
            const reachedEnd = document.body.innerText.includes("You've reached the end of the list");
            if (reachedEnd) endOfList = true;
          }
          lastHeight = currentHeight;

          if (endOfList) {
            clearInterval(scrollInterval);
            const items = document.querySelectorAll('div[role="list"] > div[role="listitem"]');
            items.forEach(item => {
              const name = item.querySelector('div[role="heading"]')?.textContent || '';
              const rating = item.querySelector('span[aria-label*="star rating"]')?.textContent || '';
              const reviews = item.querySelector('span[aria-label*="reviews"]')?.textContent.replace(/\(|\)/g, '') || '';
              const category = item.querySelector('div.fontBodySmall')?.textContent.split('·')[0]?.trim() || '';
              const address = item.querySelector('div.fontBodySmall')?.textContent.split('·').find(s => s.includes(','))?.trim() || '';
              const website = item.querySelector('a[href*="http"]')?.href || '';
              const phone = item.querySelector('div.fontBodySmall')?.textContent.split('·').find(s => s.match(/\d{3}-\d{3}-\d{4}/))?.trim() || '';
              const url = item.querySelector('a[href*="maps/place"]')?.href || '';

              if (name) {
                results.push({
                  business_name: name,
                  review_count: parseInt(reviews) || 0,
                  phone,
                  domain: website,
                  email: ''
                });
              }
            });
            resolve(results);
          }
        }, 1000);
      }
      attempts++;
    }, 1000);
  });
}

window.addEventListener('load', () => {
  if (window.location.href.includes('/maps/search/')) {
    console.log('Starting scrape');
    scrapeGoogleMaps()
      .then(data => {
        chrome.runtime.sendMessage({ action: 'scrapedData', data });
      })
      .catch(error => {
        chrome.runtime.sendMessage({ action: 'scrapeError', error: error.message });
      });
  }
});
