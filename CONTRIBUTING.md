# Contributing

When contributing to this repository, please first discuss the change you wish to make via issue, email, or any other method with the owners of this repository before making a change.

Please note we have a code of conduct, please follow it in all your interactions with the project.

## Reporting an Issue

In order to submit an issue please visit our [YouTrack](https://issues.hibernatingrhinos.com/) and create a ticket in one of the following projects:

- [RavenDB](https://issues.hibernatingrhinos.com/issues/RavenDB) - for RavenDB Server issues
- [RavenDB Clients](https://issues.hibernatingrhinos.com/issues/RDBC) - for Client API issues
- [RavenDB Documentation](https://issues.hibernatingrhinos.com/issues/RDoc) - for Documentation issues

## Setting up Development Environment

Recommended dev environment is

- VS 2022 updated to the latest version
- .NET 6 SDKs
- latest NodeJS LTS  

1. Clone this repo, open it in VS 2022.  
2. Install [NPM Task runner for VS](https://github.com/madskristensen/NpmTaskRunner)  
3. Open Task Runner Explorer (View > Other Windows > Task Runner Explorer)
    - run package.json > Defaults > install
    - run package.json > Custom > restore_compile
4. Set up src/Raven.Server as a Startup Project
5. Run it with or without debugging, console with RavenDB Server will start
6. Open http://127.0.0.1:8080/ in a browser to access Studio
7. Register your development license in the OS environment as `RAVEN_License`

## Building RavenDB artifacts

Preconditions

- .NET 6 SDKs
- latest NodeJS LTS 
- PowerShell (if you are working on Linux, you can find installation instructions [here](https://learn.microsoft.com/en-us/powershell/scripting/install/installing-powershell-on-linux)) 

1. Run `./build.ps1` (Windows) or `build.sh` (Linux)
2. After script completes, `artifacts` folder in the root of the repository will contain generated artifacts

Usually, you want to run `build` with option(s) to generate subset of all possible artifacts.  
Options are available via `-help` switch and they include

```
-WinX64     - build only Windows x64 artifacts
-WinX86     - build only Windows x86 artifacts
-LinuxX64   - build only Linux x64 artifacts
-LinuxArm64 - build only Linux Arm64 artifacts
-MacOs      - build only MacOS artifacts
-Osx        - build only OS X artifacts
-Rpi        - build only Raspberry Pi artifacts
-DontRebuildStudio - skip building studio if it was build before
-Target [TargetIds] - accepts comma-separated list of build target names; builds only for selected platforms (possible build targets: win-x64, win-x86, linux-x64, macos-x64, macos-arm64, rpi, linux-arm64)
```

Building Studio takes significant amount of time. In case you did not make any changes to the Studio,
use `DontRebuildStudio` switch to speed up build process.  
Additionally, `DontRebuildStudio` will detect if Studio has not been built yet, and will build it first time.

Hence, if you are not developing RavenDB Studio, you can use

```
-[Target]
-DontRebuildStudio
```

### Community

Community Group is available via [GitHub Issues](https://github.com/ravendb/ravendb/issues) or [Google Groups](https://groups.google.com/forum/#!forum/ravendb). Do not hesitate to join and ask for help.

## Submitting a Pull Request

Each Pull Request will be checked against the following rules:

- `cla/signed` - all commit authors need to sign a CLA. This can be done using our [CLA sign form](https://ravendb.net/contributors/cla/sign).
- `commit/whitespace` - all changed files cannot contain TABs inside them.
- `commit/message/conventions` - all commit messages (except in merge commits) must contain an issue number from our [YouTrack](https://issues.hibernatingrhinos.com) e.g. 'RavenDB-1234 Fixed issue with something'
- `tests` - this executes `build.cmd Test` on our CI to check if no constraints were violated

## Code of Conduct

### Our Pledge

We as members, contributors, and leaders pledge to make participation in our community a harassment-free experience for everyone, regardless of age, body size, visible or invisible disability, ethnicity, sex characteristics, gender identity and expression, level of experience, education, socio-economic status, nationality, personal appearance, race, religion, or sexual identity and orientation.

We pledge to act and interact in ways that contribute to an open, welcoming, diverse, inclusive, and healthy community.

### Our Standards

Examples of behavior that contributes to a positive environment for our community include:

* Demonstrating empathy and kindness toward other people
* Being respectful of differing opinions, viewpoints, and experiences
* Giving and gracefully accepting constructive feedback
* Accepting responsibility and apologizing to those affected by our mistakes, and learning from the experience
* Focusing on what is best not just for us as individuals, but for the overall community

Examples of unacceptable behavior include:

* The use of sexualized language or imagery, and sexual attention or
  advances of any kind
* Trolling, insulting or derogatory comments, and personal or political attacks
* Public or private harassment
* Publishing others' private information, such as a physical or email
  address, without their explicit permission
* Other conduct which could reasonably be considered inappropriate in a
  professional setting

### Enforcement Responsibilities

Community leaders are responsible for clarifying and enforcing our standards of acceptable behavior and will take appropriate and fair corrective action in response to any behavior that they deem inappropriate, threatening, offensive, or harmful.

Community leaders have the right and responsibility to remove, edit, or reject comments, commits, code, wiki edits, issues, and other contributions that are not aligned to this Code of Conduct, and will communicate reasons for moderation decisions when appropriate.

### Scope

This Code of Conduct applies within all community spaces, and also applies when an individual is officially representing the community in public spaces. Examples of representing our community include using an official e-mail address, posting via an official social media account, or acting as an appointed representative at an online or offline event.

### Enforcement

Instances of abusive, harassing, or otherwise unacceptable behavior may be reported to the community leaders responsible for enforcement at support@ravendb.net. All complaints will be reviewed and investigated promptly and fairly.

All community leaders are obligated to respect the privacy and security of the reporter of any incident.

### Enforcement Guidelines

Community leaders will follow these Community Impact Guidelines in determining the consequences for any action they deem in violation of this Code of Conduct:

#### 1. Correction

**Community Impact**: Use of inappropriate language or other behavior deemed unprofessional or unwelcome in the community.

**Consequence**: A private, written warning from community leaders, providing clarity around the nature of the violation and an explanation of why the behavior was inappropriate. A public apology may be requested.

#### 2. Warning

**Community Impact**: A violation through a single incident or series of actions.

**Consequence**: A warning with consequences for continued behavior. No interaction with the people involved, including unsolicited interaction with those enforcing the Code of Conduct, for a specified period of time. This includes avoiding interactions in community spaces as well as external channels like social media. Violating these terms may lead to a temporary or permanent ban.

#### 3. Temporary Ban

**Community Impact**: A serious violation of community standards, including sustained inappropriate behavior.

**Consequence**: A temporary ban from any sort of interaction or public communication with the community for a specified period of time. No public or private interaction with the people involved, including unsolicited interaction with those enforcing the Code of Conduct, is allowed during this period. Violating these terms may lead to a permanent ban.

#### 4. Permanent Ban

**Community Impact**: Demonstrating a pattern of violation of community standards, including sustained inappropriate behavior,  harassment of an individual, or aggression toward or disparagement of classes of individuals.

**Consequence**: A permanent ban from any sort of public interaction within the project community.

### Attribution

This Code of Conduct is adapted from the [Contributor Covenant][homepage], version 2.0,
available at https://www.contributor-covenant.org/version/2/0/code_of_conduct.html.

Community Impact Guidelines were inspired by [Mozilla's code of conduct enforcement ladder](https://github.com/mozilla/diversity).

[homepage]: https://www.contributor-covenant.org

For answers to common questions about this code of conduct, see the FAQ at
https://www.contributor-covenant.org/faq. Translations are available at https://www.contributor-covenant.org/translations.
