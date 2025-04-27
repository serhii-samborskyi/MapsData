function scrapeGoogleMaps() {
  return new Promise((resolve, reject) => {
    // Poll for the results pane with retries
    let attempts = 0;
    const maxAttempts = 10;
    const pollInterval = setInterval(() => {
      const scrollable = document.querySelector('div[role="list"]');
      if (scrollable || attempts >= maxAttempts) {
        clearInterval(pollInterval);
        if (!scrollable) {
          console.log('Scrollable results pane not found after max attempts.');
          resolve([]); // Return empty results instead of rejecting
          return;
        }

        // Start scraping
        let endOfList = false;
        let lastHeight = 0;
        const results = [];

        const scrollInterval = setInterval(() => {
          scrollable.scrollBy(0, 5000);
          const currentHeight = scrollable.scrollHeight;

          if (currentHeight === lastHeight) {
            const reachedEnd = document.body.innerText.includes("You've reached the end of the list");
            if (reachedEnd) {
              endOfList = true;
            }
          }
          lastHeight = currentHeight;

          if (endOfList) {
            clearInterval(scrollInterval);

            // Extract results from the list
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
                  name: `"${name.replace(/"/g, '""')}"`,
                  rating: `"${rating.replace(/"/g, '""')}"`,
                  reviews: `"${reviews.replace(/"/g, '""')}"`,
                  category: `"${category.replace(/"/g, '""')}"`,
                  address: `"${address.replace(/"/g, '""')}"`,
                  website: `"${website.replace(/"/g, '""')}"`,
                  phone: `"${phone.replace(/"/g, '""')}"`,
                  url: `"${url.replace(/"/g, '""')}"`
                });
              }
            });

            console.log(`Scraped ${results.length} results`);
            resolve(results);
          }
        }, 1000);
      }
      attempts++;
    }, 1000); // Poll every 1 second
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
