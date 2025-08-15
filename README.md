# 🍽️ GitHub Pages Food Chart

A demo project that:
- Fetches top 10 reviewed foods from DynamoDB
- Publishes the data and a chart to GitHub Pages
- Uses GitHub Actions for automation

## 🚀 Quick Start
1. Fork this repo.
2. Enable **GitHub Pages** in Settings → Pages → Source = `main` branch, root (`/`).
3. Visit your GitHub Pages URL to see the sample chart.

## 🔗 Connect to DynamoDB
1. In Settings → Secrets and Variables → Actions, add:
   - `AWS_ACCESS_KEY_ID`
   - `AWS_SECRET_ACCESS_KEY`
   - `DYNAMODB_TABLE` (optional, defaults to `FoodReviews`)
2. The GitHub Action will run nightly and update `data/top_foods.json`.

---
