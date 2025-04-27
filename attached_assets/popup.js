document.addEventListener('DOMContentLoaded', () => {
  const errorDiv = document.getElementById('error');
  const campaignsDiv = document.getElementById('campaigns');
  const statusDiv = document.getElementById('status');

  // Fetch active campaigns on popup load
  chrome.runtime.sendMessage({ action: 'getActiveCampaigns' }, response => {
    if (response.error) {
      errorDiv.textContent = response.error;
      return;
    }

    campaignsDiv.innerHTML = '';
    response.campaigns.forEach(campaign => {
      const div = document.createElement('div');
      div.className = 'campaign';
      div.innerHTML = `
        <strong>${campaign.name}</strong> (ID: ${campaign.id})<br>
        <button data-campaign-id="${campaign.id}" data-campaign-name="${campaign.name}">Start</button>
      `;
      campaignsDiv.appendChild(div);
    });

    // Add event listeners to Start buttons
    document.querySelectorAll('button[data-campaign-id]').forEach(button => {
      button.addEventListener('click', () => {
        const campaignId = button.getAttribute('data-campaign-id');
        const campaignName = button.getAttribute('data-campaign-name');
        button.disabled = true;
        statusDiv.textContent = `Processing campaign: ${campaignName}...`;
        errorDiv.textContent = '';

        chrome.runtime.sendMessage({
          action: 'startCampaign',
          campaignId,
          campaignName
        }, response => {
          button.disabled = false;
          if (response.error) {
            errorDiv.textContent = response.error;
          } else {
            statusDiv.textContent = `Campaign ${campaignName} completed.`;
          }
        });
      });
    });
  });
});
