
document.addEventListener('DOMContentLoaded', async () => {
  const campaignsDiv = document.getElementById('campaigns');
  try {
    const response = await fetch('https://your-replit-url/api/campaigns/active');
    const data = await response.json();
    
    data.campaigns.forEach(campaign => {
      const div = document.createElement('div');
      div.className = 'campaign';
      div.innerHTML = `
        <h3>${campaign.name}</h3>
        <p>Requests: ${campaign.total_requests}</p>
        <p>Contacts: ${campaign.total_contacts}</p>
      `;
      campaignsDiv.appendChild(div);
    });
  } catch (error) {
    campaignsDiv.innerHTML = 'Error loading campaigns';
  }
});
