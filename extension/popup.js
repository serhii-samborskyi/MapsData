
const API_BASE = 'https://fac10661-ee9f-4358-b944-f137d7d73a1a-00-30y5jt13c33qm.riker.replit.dev';

async function fetchActiveCampaigns() {
  const response = await fetch(`${API_BASE}/api/campaigns/active`);
  const data = await response.json();
  return data.campaigns;
}

async function startCampaign(campaign) {
  const processRequest = async () => {
    // Get available request
    const requestResponse = await fetch(`${API_BASE}/api/campaign/${campaign.name}/requests`);
    if (!requestResponse.ok) {
      if (requestResponse.status === 404) {
        // No more requests, mark campaign as completed
        await fetch(`${API_BASE}/api/campaign/${campaign.id}/complete`, {
          method: 'POST'
        });
        return false;
      }
      throw new Error('Failed to get requests');
    }

    const requestData = await requestResponse.json();
    const request = requestData.requests[0];

    // Mark request as in use
    await fetch(`${API_BASE}/api/request/${request.id}/status`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: `status=inuse`
    });

    // Here you would implement your search logic and gather data
    // For now we'll just simulate with dummy data
    const contactData = new URLSearchParams({
      campaign_id: campaign.id,
      request_id: request.id,
      business_name: 'Test Business',
      review_count: '10',
      phone: '123-456-7890',
      domain: 'test.com',
      email: 'test@test.com'
    });

    // Save contact data
    await fetch(`${API_BASE}/api/contacts`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: contactData
    });

    // Mark request as completed
    await fetch(`${API_BASE}/api/request/${request.id}/status`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: `status=completed`
    });

    return true;
  };

  while (await processRequest()) {
    // Continue processing until no more requests
    console.log('Processing next request...');
  }
}

document.addEventListener('DOMContentLoaded', async () => {
  const campaignsDiv = document.getElementById('campaigns');
  try {
    const campaigns = await fetchActiveCampaigns();
    
    campaigns.forEach(campaign => {
      const div = document.createElement('div');
      div.className = 'campaign';
      div.innerHTML = `
        <h3>${campaign.name}</h3>
        <p>Requests: ${campaign.total_requests}</p>
        <p>Contacts: ${campaign.total_contacts}</p>
        <button class="start-btn">Start Campaign</button>
      `;
      
      const startBtn = div.querySelector('.start-btn');
      startBtn.addEventListener('click', async () => {
        startBtn.disabled = true;
        startBtn.textContent = 'Processing...';
        try {
          await startCampaign(campaign);
          startBtn.textContent = 'Completed';
        } catch (error) {
          console.error('Error:', error);
          startBtn.textContent = 'Error';
        }
      });
      
      campaignsDiv.appendChild(div);
    });
  } catch (error) {
    campaignsDiv.innerHTML = 'Error loading campaigns';
    console.error('Error:', error);
  }
});
