# Bias-Free College Football Rankings

A sophisticated college football ranking system that leverages advanced statistical methodologies to generate unbiased, data-driven team and conference rankings with cutting-edge analytics.

## 🎯 Key Features

- **Two-Layer PageRank Algorithm** with enhanced EM convergence
- **Automated FBS-Only Data Processing** (134 authentic teams)
- **Comprehensive Bootstrap Sampling** for ranking confidence
- **Relative Conference Strength Scaling**
- **Advanced Neutrality Metrics** and ranking anomaly detection
- **Automated Weekly Updates** via GitHub Actions
- **Public Website** with sortable rankings and JSON API

## 🚀 Quick Start

### Local Development

1. Clone the repository
2. Install dependencies: `pip install -r pyproject.toml`
3. Set environment variable: `export CFB_API_KEY=your_api_key`
4. Run rankings: `python fbs_only_pipeline.py`

### Automated Website Deployment

1. Enable GitHub Pages in repository settings (source: `main` branch, `/site` folder)
2. Add `CFB_API_KEY` as repository secret
3. Rankings automatically update every Monday at 3:30 AM ET

## 📊 Live Rankings

- **Website**: [Enable GitHub Pages to activate](https://docs.github.com/en/pages)
- **Current Rankings JSON**: `https://yourusername.github.io/yourrepo/current.json`
- **Final Season Rankings**: `https://yourusername.github.io/yourrepo/final.json`

## 🔧 System Architecture

### Core Pipeline Components

- **Data Ingestion** (`src/ingest.py`) - College Football Data API with FBS filtering
- **Validation** (`src/validation.py`) - Industry-grade data quality checks
- **Graph Construction** (`src/graph.py`) - Two-layer directed graph modeling
- **PageRank Algorithm** (`src/pagerank.py`) - Enhanced EM convergence
- **Conference Strength** (`src/weights.py`) - Relative scaling algorithms
- **Bias Auditing** (`src/bias_audit.py`) - Neutrality metric computation
- **Bootstrap Analysis** (`src/bootstrap.py`) - Statistical confidence intervals

### Automated Publication

- **GitHub Actions** (`.github/workflows/update-rankings.yml`) - Weekly automation
- **HTML Generator** (`tools/make_html_table.py`) - Dynamic table creation
- **Static Site** (`site/`) - Professional responsive design
- **JSON API** - Real-time data access without authentication

## 📈 Mathematical Methodology

### Two-Layer PageRank Algorithm

1. **Team Graph Construction**: Victory margins weighted by venue and temporal decay
2. **Conference Graph**: Inter-conference strength relationships
3. **EM Convergence**: Iterative algorithm achieving mathematical stability
4. **Conference Injection**: Relative strength scaling for intra-conference games

### Data Integrity Framework

- **Pydantic Schema Validation** with strict type checking
- **FBS Classification Verification** against season-specific master lists
- **Duplicate Detection** and statistical outlier analysis
- **API Reliability** with exponential backoff and cached fallbacks
- **Comprehensive Edge Condition Handling** for real-world scenarios

### Bias Neutrality

- **Neutrality Metric B**: Mathematical measurement of conference bias
- **Auto-Tuning Lambda**: Recency decay parameter optimization
- **Bootstrap Confidence**: Statistical uncertainty quantification

## 🗂️ Data Sources

- **Primary**: College Football Data API (official game results)
- **Classification**: Season-specific FBS master lists
- **Validation**: Industry-grade data integrity checks
- **Backup**: Cached data with automatic fallback mechanisms

## 📋 Pipeline Types

### Live Pipeline
- Updates weekly during season
- Uses previous week's ratings as starting point
- Optimized for speed and current season analysis

### Retrospective Pipeline
- Complete season analysis with hindsight
- Full bootstrap uncertainty quantification
- Final definitive rankings after postseason

## 🛠️ Advanced Features

### Industry-Grade Data Validation
- Schema validation with CI blocking
- FBS/FCS classification guards
- Season consistency and game status validation
- API timeout handling and retry logic
- End-to-end smoke testing

### Automated Website System
- Monday updates during football season
- Responsive design with professional styling
- Sortable tables with DataTables integration
- JSON API endpoints for external applications
- Automatic switch to final rankings after season

### Conference Strength Analysis
- Relative scaling based on inter-conference performance
- Historical tracking and trajectory analysis
- Real-time bias metric computation

## 🎮 Usage Examples

### Run Current Week Rankings
```python
from src.live_pipeline import run_current_week
result = run_current_week(season=2024)
```

### Generate Final Season Rankings
```python
from fbs_only_pipeline import run_fbs_only_pipeline
result = run_fbs_only_pipeline(season=2024, week=15)
```

### Access JSON API
```bash
curl https://yourusername.github.io/yourrepo/current.json
```

### Bias Audit Analysis
```python
from src.bias_audit import BiasAudit
audit = BiasAudit()
metrics = audit.get_latest_metrics()
```

## 📊 Output Formats

- **CSV Exports** (`exports/*.csv`) - Spreadsheet-compatible rankings
- **JSON Exports** (`exports/*.json`) - API-ready data with metadata
- **HTML Tables** (`site/*.html`) - Public web display
- **Bias Reports** (`reports/*.json`) - Neutrality analysis

## 🔍 Quality Assurance

### Comprehensive Testing
- Unit tests for mathematical algorithms
- Integration tests for full pipeline
- Golden file tests for consistency verification
- End-to-end smoke tests with BYU-style metrics

### Data Quality Controls
- Real-time validation with error blocking
- Checksum verification for data integrity
- Automated missing alias detection
- Statistical outlier monitoring

## 📚 Documentation

- **DEPLOYMENT.md** - Complete deployment guide
- **CHANGELOG.md** - Version history and updates
- **Config Documentation** - Parameter tuning guide
- **API Reference** - Endpoint specifications

## 🤝 Contributing

1. Fork the repository
2. Create feature branch
3. Implement changes with tests
4. Ensure all validation passes
5. Submit pull request

## 📜 License

This project implements advanced statistical methodologies for educational and analytical purposes. See LICENSE file for details.

## 🔗 Links

- [College Football Data API](https://collegefootballdata.com/)
- [PageRank Algorithm](https://en.wikipedia.org/wiki/PageRank)
- [GitHub Pages Documentation](https://docs.github.com/en/pages)
- [Bootstrap Statistical Methods](https://en.wikipedia.org/wiki/Bootstrapping_(statistics))

---

*Automated rankings update every Monday during football season. System designed for zero-maintenance operation with industry-grade reliability.*