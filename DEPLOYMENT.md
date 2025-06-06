# Automated Ranking Publication System

This system automatically publishes college football rankings every Monday during the season, providing a public website with current rankings that updates without manual intervention.

**Developer Note**: For comprehensive understanding of the system architecture and implementation details, review the **[Guiding Docs](./Guiding%20Docs/README.md)** folder before deployment.

## System Overview

The automated publication system consists of:

1. **GitHub Actions Workflow** - Runs every Monday at 3:30 AM ET
2. **Static Site Generator** - Creates HTML tables from ranking JSON data
3. **GitHub Pages** - Hosts the public website
4. **JSON API** - Provides programmatic access to ranking data

## Deployment Steps

### 1. Enable GitHub Pages

In your repository settings:
- Go to Pages section
- Set source to "Deploy from a branch"
- Select branch: `main`
- Select folder: `/site`
- Save settings

Your site will be available at: `https://username.github.io/repository-name`

### 2. Configure API Secret

Add your College Football Data API key as a repository secret:
- Go to Settings → Secrets and variables → Actions
- Click "New repository secret"
- Name: `CFB_API_KEY`
- Value: Your API key from CollegeFootballData.com

### 3. Workflow Schedule

The system automatically:
- **During Season (August-January)**: Updates rankings every Monday
- **Off-Season (February-July)**: Displays final season rankings
- **Manual Trigger**: Can be run manually from Actions tab

## Site Structure

```
site/
├── index.html          # Landing page with system information
├── current.html        # Current rankings table (sortable)
├── current.json        # Current rankings API endpoint
├── final.html          # Final season rankings table
├── final.json          # Final season rankings API endpoint
└── style.css           # Professional styling
```

## API Endpoints

### Current Rankings
```
GET https://username.github.io/repository-name/current.json
```

### Final Season Rankings
```
GET https://username.github.io/repository-name/final.json
```

## Features

### Automatic Updates
- Detects current week and season automatically
- Runs complete data validation pipeline
- Generates new HTML and JSON files
- Commits and pushes changes to repository
- GitHub Pages automatically deploys updates

### Responsive Design
- Clean, professional appearance
- Sortable tables with search functionality
- Mobile-responsive layout
- Conference-aware color coding for top teams

### Data Integrity
- Uses same industry-grade validation as manual runs
- Comprehensive error handling and logging
- Fallback mechanisms for API issues
- Automatic retry logic with exponential backoff

### Season Management
- Automatically switches to final rankings after postseason
- Displays current rankings during active season
- Handles bye weeks and schedule changes gracefully

## Manual Operations

### Trigger Update Manually
1. Go to Actions tab in GitHub
2. Select "Weekly Rankings Update"
3. Click "Run workflow"
4. Optionally specify season/week

### Generate Final Rankings
Final rankings are automatically generated when the system detects the season has ended. To manually trigger:
1. Run the retro pipeline locally: `python fbs_only_pipeline.py`
2. The workflow will detect and publish final rankings on next run

### Troubleshooting

**Workflow fails with API errors:**
- Check that CFB_API_KEY secret is correctly set
- Verify API key is valid at CollegeFootballData.com

**Rankings not updating:**
- Check Actions tab for workflow execution logs
- Ensure repository has write permissions for Actions
- Verify /site folder exists and is properly structured

**HTML table issues:**
- Check tools/make_html_table.py for errors
- Verify JSON data format matches expected schema
- Test table generator locally

## Development

### Local Testing
```bash
# Generate HTML table
python tools/make_html_table.py exports/2024_retro.json > test_table.html

# Test site locally
cd site
python -m http.server 8000
# Visit http://localhost:8000
```

### Customization
- Modify `site/style.css` for different styling
- Edit `tools/make_html_table.py` for table layout changes
- Update `.github/workflows/update-rankings.yml` for schedule changes

## Benefits

1. **Zero Maintenance**: Rankings update automatically without intervention
2. **Always Current**: Shows latest rankings every Monday during season
3. **Public Access**: No API keys required for users to view rankings
4. **Fast Loading**: Static files load instantly from GitHub's CDN
5. **API Access**: JSON endpoints for building applications
6. **Mobile Friendly**: Responsive design works on all devices

The system provides a professional, maintenance-free way to publish and share your bias-free college football rankings with the world.