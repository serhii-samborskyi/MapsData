
const API_BASE = 'http://0.0.0.0:8000';

async function fetchActiveCampaigns() {
  const response = await fetch(`${API_BASE}/api/campaigns/active`);
  return response.json();
}

document.addEventListener('DOMContentLoaded', async () => {
  const campaignsDiv = document.getElementById('campaigns');
  const errorDiv = document.getElementById('error');
  const statusDiv = document.getElementById('status');

  try {
    const data = await fetchActiveCampaigns();

    data.campaigns.forEach(campaign => {
      const div = document.createElement('div');
      div.className = 'campaign';
      div.innerHTML = `
        <strong>${campaign.name}</strong><br>
        <button class="start-btn" data-id="${campaign.id}" data-name="${campaign.name}">Start</button>
      `;

      const startBtn = div.querySelector('.start-btn');
      startBtn.addEventListener('click', () => {
        startBtn.disabled = true;
        startBtn.textContent = 'Processing...';
        statusDiv.textContent = 'Starting campaign...';
        errorDiv.textContent = '';

        chrome.runtime.sendMessage({
          action: 'startCampaign',
          campaignId: campaign.id,
          campaignName: campaign.name
        }, response => {
          if (response.error) {
            errorDiv.textContent = response.error;
            startBtn.textContent = 'Error';
          } else {
            statusDiv.textContent = 'Campaign completed!';
            startBtn.textContent = 'Completed';
          }
          startBtn.disabled = false;
        });
      });

      campaignsDiv.appendChild(div);
    });
  } catch (error) {
    errorDiv.textContent = 'Failed to load campaigns';
    console.error(error);
  }
});
