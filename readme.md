# RavenDB - the premier NoSQL database for .NET

This repository contains source code for [RavenDB](http://ravendb.net/) document database.

Build Status
------------
| Version | Windows | Linux |
|:-------:|:-------|:------:|
| 3.0 | ![](http://teamcity.hibernatingrhinos.com/app/rest/builds/buildType:(id:RavenDBTests_30Tests)/statusIcon) | N/A |
| 3.5 | ![](http://teamcity.hibernatingrhinos.com/app/rest/builds/buildType:(id:RavenDBTests_35Tests)/statusIcon) | N/A |
| 4.0 | ![](http://teamcity.hibernatingrhinos.com/app/rest/builds/buildType:(id:RavenDBTests_40TestsWindows)/statusIcon) | ![](http://teamcity.hibernatingrhinos.com/app/rest/builds/buildType:(id:RavenDBTests_40TestsLinux)/statusIcon) |

New to RavenDB?
---------------
Check out our [Getting started page](http://ravendb.net/docs/article-page/3.5/csharp/start/getting-started).

How to download?
-----------------------
| Stable | [download](http://ravendb.net/downloads) | [NuGet](https://www.nuget.org/packages/RavenDB.Server) |
|:-------:|:-------:|:-------:|
| Unstable | [download](http://ravendb.net/downloads/builds) | [NuGet](https://www.nuget.org/packages/RavenDB.Server) |
| .NET Client | [download](http://ravendb.net/downloads) | [NuGet](https://www.nuget.org/packages/RavenDB.Client) |
| Java Client | [download](http://ravendb.net/downloads) | [Maven](http://search.maven.org/#search%7Cgav%7C1%7Cg%3A%22net.ravendb%22%20AND%20a%3A%22ravendb-client%22) |

What's new?
-----------
Our latest list of changes can always be found [here](http://ravendb.net/docs/article-page/3.5/csharp/start/whats-new).

Found a bug?
------------
You can create issues at our [YouTrack](http://issues.hibernatingrhinos.com).

How to build?
-------------
Requirements:

- .NET 4.5+
- [TypeScript 1.5.4](http://go.microsoft.com/fwlink/?LinkID=619584)
- [Node.js](https://nodejs.org/en/download/)
- [Gulp](https://github.com/gulpjs/gulp/blob/master/docs/getting-started.md)
- [WiX Toolset 3.7 or higher](http://wixtoolset.org/releases/)

The easiest way to build is to execute `build.cmd` or `quick.ps1`. Just ensure that you can execute PowerShell scripts, if you can't then you might want to execute `Set-ExecutionPolicy Unrestricted` in PowerShell as an Administrator.

Want to contribute?
-------------------
Each Pull Request will be checked against following rules:

- `cla/signed` - all commit authors need to sign CLA. This can be done using our [CLA sign form](http://ravendb.net/contributors/cla/sign)
- `commit/whitespace` - all changed files cannot contain TABs inside them. Before doing any work we suggest executing our `git_setup.cmd`. This will install git pre-commit hook that will normalize all whitespaces during commit
- `commit/message/conventions` - all commit messages (except in merge commits) must contain issue number from our [YouTrack](http://issues.hibernatingrhinos.com) e.g. 'RavenDB-1234 Fixed issue with something'
- `tests` - this executes `build.cmd Test` on our CI to check if no constraints were voilated

Need help?
----------
If you have any questions please visit our [community group](http://groups.google.com/group/ravendb/).