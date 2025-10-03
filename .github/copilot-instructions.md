# RavenDB Copilot Instructions

## Repository Overview

RavenDB is a modern ACID-compliant NoSQL document database written in C# (.NET 8.0) with TypeScript/React web studio. Open source (AGPLv3), supports Windows/Linux/MacOS/containers.

**Key Info:** Large repo, .NET, Node.js >=20.0.0, React/TypeScript UI

## Build & Validation (CRITICAL)

### Prerequisites
- .NET SDK 8.0.413 (exact version in `global.json`)
- Node.js >=20.0.0 LTS 
- PowerShell (Linux: `sudo ./install_build_prerequisites.sh`)

### Build Commands (VALIDATED - Times: Full=20min, Server=5min, FastTests=2min)

```bash
# Full build (20+ minutes)
./build.ps1 -LinuxX64                            # Windows
./build.sh -LinuxX64                             # Linux/MacOS

# RECOMMENDED: .NET Build Only
dotnet build -c Release

# Studio-only (10+ minutes)
./build.ps1 -JustStudio

# Studio development
cd src/Raven.Studio
npm ci && npm run restore_compile   # Initial setup
npm run webpack-watch                    # Development mode
```

### Test Commands
```bash
# Fast tests (2-5 min) - RUN THESE FREQUENTLY
cd test/FastTests && dotnet test --configuration Release

# Studio tests
cd src/Raven.Studio && npm test          # No need to run studio tests

# Validation (required for CI)
cd src/Raven.Studio && npm run prettier && npm run lint         # Studio
grep -r "\t" --include="*.cs" src/                              # Check for tabs (CI fails)
```

## Project Layout & Architecture

### Core Components
- `src/Raven.Server/` - Main database server, start here for DB changes
- `src/Raven.Client/` - .NET client library  
- `src/Raven.Studio/` - React/TypeScript web UI (takes longest to build)
- `src/Voron/` - Storage engine
- `src/Sparrow/` - Performance utilities
- `test/FastTests/` - Unit tests (run frequently, 2-5 min)
- `test/SlowTests/` - Integration tests

### Key Files
- `global.json` - .NET SDK version
- `RavenDB.sln` - Main solution
- `.editorconfig` - Code formatting rules
- `RavenDB.ruleset` - C# analysis rules
- `src/Raven.Studio/package.json` - Studio npm deps (Node >=20.0.0)
- `.github/pull_request_template.md` - Pull Request Template

### GitHub Actions (in `.github/workflows/`)
**WILL FAIL CI:**
- Tabs in source files
- Missing CLA signature  
- Commit messages without "RavenDB-#### Description" format
- Test failures

## Critical Development Workflow

### Before Making Changes
1. **Run FastTests baseline:** `cd test/FastTests && dotnet test --configuration Release`
2. **Verify .NET version:** `dotnet --version` (must match the `global.json` one)
3. **For Studio work:** `cd src/Raven.Studio && npm ci && npm run restore_compile`

### Efficient Development Commands
```bash
# Server changes (fastest iteration)
dotnet build -c Release

# Studio changes (use watch mode)
cd src/Raven.Studio && npm run webpack-watch

# Quick validation
cd test/FastTests && dotnet test           # Run after each change
```

### Common Issues & Solutions
- **Build hangs:** Kill node processes, delete `node_modules`, `npm ci`
- **.NET mismatch:** Verify `global.json` matches installed SDK  
- **Memory issues:** Studio build requires 4GB+ RAM
- **CI failures:** Check for tabs in files, verify commit message format
- **Documentation:** Available at http://docs.ravendb.net website

### Development Server Setup
1. `dotnet build -c Release`
2. Set startup project: `src/Raven.Server`  
3. Run - access Studio at http://127.0.0.1:8080/
4. Set `RAVEN_LICENSE` env variable for dev license

## Agent Instructions

**TRUST THESE INSTRUCTIONS** - Commands are validated. Search only if instructions are incomplete.

**Key Principles:**
- FastTests (2-5 min) are your primary validation tool
- Studio builds are slowest (10+ min) - use `dotnet build -c Release` when possible
- Server changes are fastest to test
- Always use Pull Request Template (markdown file: `.github/pull_request_template.md`)
- Always check commit message format: "RavenDB-#### Description"
- Never update any packages versions
- No tabs in source files (CI will fail)