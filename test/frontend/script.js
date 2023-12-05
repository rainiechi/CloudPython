// Function to fetch data and generate chart
function generateChart(data, elementId, label) {
    const ctx = document.getElementById(elementId).getContext('2d');
    const chart = new Chart(ctx, {
        type: 'line', // or 'bar' or other chart types
        data: {
            labels: data.labels, // Array of labels (x-axis)
            datasets: [{
                label: label,
                backgroundColor: 'rgba(0, 123, 255, 0.5)',
                borderColor: 'rgb(0, 123, 255)',
                data: data.times // Array of times (y-axis)
            }]
        },
        options: {
            scales: {
                y: {
                    beginAtZero: true
                }
            }
        }
    });
}

// JSON object to pass to Lambda Function
const json = {
    s3Bucket: "test.bucket.462562f23.bm",
    s3Key: "data.csv"
};

// Fetch the data from your API Gateway and generate the charts
fetch('https://vxawyzuez0.execute-api.us-east-2.amazonaws.com/CSVProcessor/', {
    method: 'POST', // Specify the method to match your curl command
    headers: {
        'Content-Type': 'application/json'
    },
    body: JSON.stringify(json) // Stringify the JSON payload
})
.then(response => {
    if (!response.ok) {
        throw new Error('Network response was not ok ' + response.statusText);
    }
    return response.json();
})
.then(data => {
    const labels = Object.keys(data).filter(key => key.includes('runtime'));
    const times = labels.map(label => data[label]);
    
    generateChart({ labels: labels, times: times }, 'java-chart', 'Java Performance');
})
.catch(error => console.error('There has been a problem with your fetch operation:', error));


// fetch('https://your-api-gateway-url/python')
//     .then(response => response.json())
//     .then(data => generateChart(data, 'python-chart', 'Python Performance'));
