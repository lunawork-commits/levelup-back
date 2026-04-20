(function () {
  var hasData = SENTIMENT_DATA.some(function (d) { return d.count > 0; });
  var ctx = document.getElementById('sentimentChart');
  if (!ctx || !hasData) return;
  new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: SENTIMENT_DATA.map(function (d) { return d.label; }),
      datasets: [{
        data: SENTIMENT_DATA.map(function (d) { return d.count; }),
        backgroundColor: SENTIMENT_DATA.map(function (d) { return d.color; }),
        borderWidth: 2,
      }],
    },
    options: {
      plugins: { legend: { display: false } },
      cutout: '65%',
      animation: { duration: 500 },
    },
  });
})();
