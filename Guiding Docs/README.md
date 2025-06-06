# Developer Guidance Documentation

This folder contains comprehensive documentation for developers working on the college football ranking system. Read these documents in order to understand the system architecture, data models, and implementation approach.

## 🎯 Start Here: Core Understanding

### 1. [Purpose_and_Vision.md](./Purpose_and_Vision.md)
**Essential first read** - Explains the core philosophy, architectural overview, and design principles that drive every technical decision in this system.

### 2. [Rating_System.md](./Rating_System.md)
**Algorithm deep-dive** - How the two-layer PageRank algorithm works, conference strength injection, and bias elimination methodology.

## 🔌 API Integration & Authentication

### 3. [API_README.md](./API_README.md)
**Technical implementation** - Complete guide to integrating with the College Football Data API using the official cfbd-python library.

### 4. [CFBD_API_Key_Setup_Guide.md](./CFBD_API_Key_Setup_Guide.md)
**Authentication setup** - Step-by-step guide for obtaining and configuring your College Football Data API key.

## 📊 CFBD Foundational Models

These documents explain the official cfbd-python library models that replace brittle dictionary lookups with clean attribute access:

### 5. [Team.md](./Team.md)
**Team model** - Authoritative team data with clean attributes like `team.school`, `team.conference`, `team.classification`

### 6. [Game.md](./Game.md)
**Game model** - Clean game data access with `game.home_team`, `game.away_team`, `game.home_points` instead of dictionary lookups

### 7. [Conference.md](./Conference.md)
**Conference model** - Official conference data for validation and strength calculations

### 8. [TeamRecord.md](./TeamRecord.md)
**TeamRecord model** - Team performance records for comprehensive validation and quality win calculations

## 🏗️ Implementation Philosophy

This system follows a **validation-first approach**:

- **Use authentic data sources**: Official CFBD API with proper authentication
- **Fail-fast validation**: Catch data issues before they affect rankings
- **Clean object models**: Use foundational models instead of brittle dictionary access
- **Comprehensive testing**: Every component validates against authentic game data

## 📁 Documentation Structure

```
Guiding Docs/
├── README.md                    # This index file
├── Purpose_and_Vision.md        # Core philosophy and architecture
├── Rating_System.md             # Algorithm methodology
├── API_README.md                # Technical API integration
├── CFBD_API_Key_Setup_Guide.md  # Authentication setup
├── Team.md                      # Team model documentation
├── Game.md                      # Game model documentation
├── Conference.md                # Conference model documentation
└── TeamRecord.md                # TeamRecord model documentation
```

## 🚀 Getting Started Workflow

1. Read **Purpose_and_Vision.md** to understand system goals
2. Review **Rating_System.md** to understand the algorithm
3. Follow **CFBD_API_Key_Setup_Guide.md** to configure authentication
4. Study the model documentation (Team.md, Game.md, etc.) to understand data access patterns
5. Reference **API_README.md** for implementation details

This documentation ensures every developer understands both the "why" and "how" of the bias-free ranking system.