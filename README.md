# 🚀 Polymarket Data Pipeline

A comprehensive four-stage data pipeline for extracting, processing, and analyzing Polymarket prediction market data using Jina Reader API and Gemini AI.

## 🗺️ Pipeline Overview

This pipeline transforms raw Polymarket data into structured, analyzable insights through four distinct stages:

### Stage 1: The Foundation 🏗️

- **Purpose**: Raw data extraction from Polymarket
- **Technology**: Jina Reader API
- **Output**: Raw markdown/JSON data (`jina_polymarket_data.json`)

### Stage 2: The Intelligence Layer 🧠

- **Purpose**: AI-powered data structuring and parsing
- **Technology**: Google Gemini AI
- **Output**: Clean, structured JSON with market groupings (`structured_polymarket_data.json`)

### Stage 3: The Automation & Storage Layer 💾

- **Purpose**: Historical data snapshots and storage
- **Technology**: File system automation
- **Output**: Dated snapshots in `history/` directory

### Stage 4: The Automation Hub 🤖

- **Purpose**: Scheduled automation and data distribution (Planned)
- **Technology**: Cron jobs, APIs, alert systems
- **Output**: Production-ready automation system

## 📁 Project Structure

```
polymarket-jina/
├── main_pipeline.py          # Main orchestrator
├── jina_reader_api.py        # Stage 1: Jina Reader API integration
├── ai_parser.py              # Stage 2: Gemini AI parser
├── requirements.txt          # Python dependencies
├── run_pipeline.sh           # Automation script for cron jobs
├── .env                      # Environment variables (create this)
├── .gitignore               # Git ignore file
├── history/                  # Stage 3: Historical snapshots
│   └── structured-data-YYYY-MM-DD.json
├── jina_polymarket_data.json        # Stage 1 output
├── structured_polymarket_data.json  # Stage 2 output
└── README.md                 # This file
```

## 🚀 Quick Start

### 1. Setup Environment

```bash
# Clone the repository
git clone https://github.com/nelithb/polymarket-trendwatch.git
cd polymarket-trendwatch

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure API Keys

Create a `.env` file in the project root:

```bash
# .env
GEMINI_API_KEY=your_gemini_api_key_here
```

**Get your Gemini API key:**

1. Visit [Google AI Studio](https://makersuite.google.com/app/apikey)
2. Create a new API key
3. Add it to your `.env` file

### 3. Run the Pipeline

#### Run All Stages

```bash
python main_pipeline.py
```

#### Run Specific Stages

```bash
# Run only Stage 1 (raw data extraction)
python main_pipeline.py --stages 1

# Run Stages 1 and 2 (raw data + AI parsing)
python main_pipeline.py --stages 1 2

# Run Stage 3 only (if structured data exists)
python main_pipeline.py --stages 3
```

#### Run with Custom API Key

```bash
python main_pipeline.py --gemini-key your_api_key_here
```

## 📊 Output Files

### Stage 1 Outputs

- `jina_polymarket_content.txt` - Raw text content from Polymarket
- `jina_polymarket_data.json` - Structured JSON from Jina Reader API

### Stage 2 Outputs

- `structured_polymarket_data.json` - Clean, AI-processed data with market groupings

### Stage 3 Outputs

- `history/structured-data-YYYY-MM-DD.json` - Daily snapshots for historical tracking

### Stage 4 Outputs

- `automation_status.json` - Pipeline status and automation metadata

## 🔄 Automation Setup

### Daily Cron Job

1. **Make the script executable:**

```bash
chmod +x run_pipeline.sh
```

2. **Add to crontab (runs daily at 9 AM):**

```bash
crontab -e
```

Add this line:

```bash
0 9 * * * /path/to/your/project/run_pipeline.sh >> /path/to/your/project/pipeline.log 2>&1
```

3. **Verify cron job:**

```bash
crontab -l
```

### Manual Automation

```bash
# Run the automation script manually
./run_pipeline.sh
```

## 🧪 Testing

### Test Individual Components

```bash
# Test Stage 1 (Jina Reader API)
python -c "from jina_reader_api import JinaReaderAPI; api = JinaReaderAPI(); print('API Test:', api.test_api_connection())"

# Test Stage 2 (AI Parser)
python -c "from ai_parser import AIPolymarketParser; parser = AIPolymarketParser(); print('AI Parser initialized successfully')"
```

### Test with Sample Data

The AI parser includes built-in sample data for testing without API calls:

```bash
# Test AI parsing with sample data
python ai_parser.py --test-sample
```

## 📈 Data Structure

### Structured Output Format

The final structured data (`structured_polymarket_data.json`) contains:

```json
{
  "metadata": {
    "timestamp": "2024-01-01T12:00:00Z",
    "source": "polymarket.com",
    "total_markets": 150,
    "processing_stage": "ai_parsed"
  },
  "markets": [
    {
      "title": "Will X happen?",
      "description": "Market description",
      "outcomes": ["Yes", "No"],
      "current_prices": [0.65, 0.35],
      "volume_24h": 10000,
      "end_date": "2024-12-31",
      "group_title": "Elections 2024", // Optional grouping
      "category": "politics"
    }
  ]
}
```

## 🔧 Configuration

### Environment Variables

| Variable         | Description           | Required                    |
| ---------------- | --------------------- | --------------------------- |
| `GEMINI_API_KEY` | Google Gemini API key | Yes                         |
| `JINA_API_URL`   | Jina Reader API URL   | No (defaults to production) |

### Pipeline Options

- **Stage Selection**: Run specific stages with `--stages` argument
- **API Key Override**: Use `--gemini-key` for custom API key
- **Verbose Logging**: Built-in logging with progress tracking

## 🛠️ Troubleshooting

### Common Issues

1. **API Key Errors**

   ```bash
   # Check if API key is set
   echo $GEMINI_API_KEY

   # Or check .env file
   cat .env
   ```

2. **Permission Errors**

   ```bash
   # Make scripts executable
   chmod +x run_pipeline.sh
   ```

3. **Virtual Environment Issues**

   ```bash
   # Reactivate environment
   source venv/bin/activate
   pip install -r requirements.txt
   ```

4. **Cron Job Not Running**

   ```bash
   # Check cron logs
   tail -f /var/log/cron

   # Test cron script manually
   ./run_pipeline.sh
   ```

### Log Files

- **Pipeline Logs**: Check `pipeline.log` for automation runs
- **Python Logs**: Built-in logging in `main_pipeline.py`
- **Cron Logs**: System cron logs for automation issues

## 🔮 Future Enhancements

### Stage 4: Automation Hub (Planned)

- **Scheduled Data Collection**: Automated daily/weekly runs
- **Alert Systems**: Price movement notifications
- **Data Distribution APIs**: REST API for external access
- **Dashboard Integration**: Real-time data visualization
- **Machine Learning**: Predictive analytics and trend analysis

### Additional Features

- **Multi-Source Integration**: Support for other prediction markets
- **Advanced Analytics**: Statistical analysis and insights
- **Data Export**: CSV, Excel, and API formats
- **User Interface**: Web dashboard for data exploration

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📞 Support

For questions, issues, or contributions:

- **GitHub Issues**: [Create an issue](https://github.com/nelithb/polymarket-trendwatch/issues)
- **Email**: nelith.bandularatne@gmail.com

---

**🚀 Happy Data Mining!**

_Transform raw market chaos into structured insights with the power of AI._
