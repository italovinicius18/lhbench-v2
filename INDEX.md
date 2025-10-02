# 📚 Lakehouse Benchmark - Documentation Index

Quick navigation guide for all project documentation.

---

## 🚀 Getting Started

1. **[GETTING_STARTED.md](GETTING_STARTED.md)** ⭐ START HERE
   - 3-command quick start
   - What you get out of the box
   - Testing instructions

2. **[QUICKSTART.md](docs/QUICKSTART.md)**
   - Detailed setup guide (5 minutes)
   - Troubleshooting common issues
   - Configuration tips

3. **[README.md](README.md)**
   - Full project documentation
   - Feature overview
   - Commands reference

---

## 📖 Understanding the System

4. **[ARCHITECTURE.md](ARCHITECTURE.md)**
   - System architecture diagrams
   - Data flow (Medallion)
   - Component design
   - Storage layout

5. **[PROJECT_SUMMARY.md](PROJECT_SUMMARY.md)**
   - What's implemented ✅
   - What's pending 🏗️
   - Code structure
   - Next steps

6. **[STATUS.md](STATUS.md)**
   - Current capabilities
   - Pending work
   - Metrics & statistics
   - Recommendations

---

## 🛠️ Using the System

### Quick Reference
```bash
# Setup
make setup

# Build & Start
make build && make up

# Run Benchmark
make benchmark
```

### Available Commands

**Makefile Commands** (30+ commands)
```bash
make help              # Show all commands
make setup             # Initial setup
make build             # Build images
make up                # Start services
make down              # Stop services
make benchmark         # Run full benchmark
make benchmark-bronze  # Bronze phase only
make logs              # View logs
make status            # Check services
make spark-ui          # Open Spark UI
make clean             # Clean all
```

**Shell Script**
```bash
./run_benchmark.sh        # Interactive mode
./run_benchmark.sh start  # Start services
./run_benchmark.sh stop   # Stop services
./run_benchmark.sh logs   # View logs
```

---

## 📂 Code Organization

### Python Scripts
```
scripts/
├── run_benchmark.py          # Main orchestrator
├── phases/
│   ├── bronze_phase.py       # ✅ Data generation
│   ├── silver_phase.py       # 🏗️ Framework conversion
│   ├── gold_phase.py         # 🏗️ Query benchmark
│   └── report_phase.py       # 🏗️ Report generation
├── orchestrator/
│   ├── state_manager.py      # ✅ Idempotency
│   └── task_runner.py        # ✅ Spark execution
└── utils/
    ├── config_loader.py      # ✅ .env parsing
    ├── logger.py             # ✅ Logging
    └── metrics_collector.py  # ✅ Metrics
```

### Spark Jobs
```
spark_jobs/
├── common/
│   ├── spark_session_builder.py  # ✅ Framework-aware
│   └── cache_manager.py          # ✅ Caching
├── bronze/                       # ✅ Uses tpchgen-cli
├── silver/                       # 🏗️ TODO
│   ├── iceberg/
│   ├── delta/
│   └── hudi/
└── gold/                         # 🏗️ TODO
```

### Configuration
```
.env.example                  # ✅ 100+ settings
docker-compose.yml            # ✅ Services
docker/                       # ✅ 4 Dockerfiles
requirements.txt              # ✅ Python deps
```

---

## 🎯 Implementation Status

### ✅ Complete (Ready to Use)
- Infrastructure & orchestration
- Bronze phase (data generation)
- State management
- Monitoring & logging
- Documentation

### 🏗️ Pending (Needs Implementation)
- Silver phase Spark jobs (Iceberg, Delta, Hudi)
- Gold phase query execution
- Report generation
- Testing suite

**See [PROJECT_SUMMARY.md](PROJECT_SUMMARY.md) for details**

---

## 📊 Performance & Scale

### tpchgen-cli Performance
```
SF1   (1GB):    ~2 seconds
SF10  (10GB):   ~10 seconds
SF100 (100GB):  ~1.5 minutes
SF1000 (1TB):   ~10 minutes
```

### Resource Requirements
```
Minimum:  16GB RAM, 50GB disk
Recommended:  32GB RAM, 200GB disk
```

---

## 🐛 Troubleshooting

Common issues and solutions in:
- [QUICKSTART.md](docs/QUICKSTART.md#common-issues--solutions)
- [README.md](README.md#-troubleshooting)

Quick fixes:
```bash
# Reset state
make reset

# Clean cache
make clean-cache

# Full cleanup
make clean

# Rebuild
make build && make up
```

---

## 🤝 Contributing

Areas needing implementation:
1. Silver phase conversions (HIGH PRIORITY)
2. Gold phase queries (HIGH PRIORITY)
3. Report generation (MEDIUM)
4. Testing (MEDIUM)

See [STATUS.md](STATUS.md#-contributing) for details.

---

## 📞 Quick Links

- **Spark Master UI**: http://localhost:8080
- **Worker 1 UI**: http://localhost:8081
- **Worker 2 UI**: http://localhost:8082

---

## 📝 Documentation Files

```
Root Level:
├── GETTING_STARTED.md  ⭐ Start here
├── README.md           📖 Main docs
├── ARCHITECTURE.md     🏗️ System design
├── PROJECT_SUMMARY.md  📊 Status
├── STATUS.md           ✅ Progress
└── INDEX.md            📚 This file

docs/:
└── QUICKSTART.md       🚀 Step-by-step

Configuration:
├── .env.example        ⚙️ Settings
├── docker-compose.yml  🐳 Services
├── Makefile           🛠️ Commands
└── run_benchmark.sh    🎯 Runner
```

---

**Quick Start**: [GETTING_STARTED.md](GETTING_STARTED.md)  
**Full Docs**: [README.md](README.md)  
**Architecture**: [ARCHITECTURE.md](ARCHITECTURE.md)  
**Status**: [STATUS.md](STATUS.md)
