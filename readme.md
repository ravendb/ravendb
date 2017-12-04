# RavenDB - An ACID NoSQL Document Database

This repository contains source code for the [RavenDB](https://ravendb.net/) document database. With a RavenDB database you can set up a NoSQL data architecture or add a NoSQL layer to your current relational database. 

![RavenDb Studio](docs/readmeScreenshot.png)

Supported Platforms
--------------

- Windows
- Linux
- Docker
- Mac OS X
- Raspberry Pi

Grab Your License and Latest Version
------------------------
**Download the latest version of [RavenDB](https://ravendb.net/downloads#server/dev)**

Getting Started
--------------
Install and [set up your database](https://ravendb.net/docs/article-page/latest/csharp/start/getting-started).

Learn RavenDB Quickly 
------------
[RavenDB Bootcamp](https://ravendb.net/learn) is a free, self-directed learning course. In just three units you will learn how to use RavenDB to create fully-functional, real-world programs with NoSQL Databases. If you are unfamiliar with NoSQL, it’s okay. We will provide you with all the information you need.

Stay Updated on New Developments
------------------
We are always adding new features to improve your RavenDB experience. Check out [our latest improvements](https://ravendb.net/docs/article-page/latest/csharp/start/whats-new), updated weekly. 

Documentation
------------
Access [full documentation](https://ravendb.net/docs/article-page/latest/csharp) for RavenDB. Like our database, it is easy to use. 

Where to Ask for Help
---------------------
If you have any questions, or need further assistance, you can [contact us directly](https://ravendb.net/contact).

Report an Issue
---------------
You can create issues and track them at our [YouTrack](http://issues.hibernatingrhinos.com/) page.

RavenDB Developer Community Group
---------------------------------
If you have any questions please visit our [community group](http://groups.google.com/group/ravendb/). The solutions for the most common challenges are available. You are welcome to join!

Submit a Pull Request
----------------------
Each Pull Request will be checked against following rules:

- `cla/signed` - all commit authors need to sign CLA. This can be done using our [CLA sign form](http://ravendb.net/contributors/cla/sign)
- `commit/whitespace` - all changed files cannot contain TABs inside them. Before doing any work we suggest executing our `git_setup.cmd`. This will install git pre-commit hook that will normalize all whitespaces during commit
- `commit/message/conventions` - all commit messages (except in merge commits) must contain issue number from our [YouTrack](http://issues.hibernatingrhinos.com) e.g. 'RavenDB-1234 Fixed issue with something'
- `tests` - this executes `build.cmd Test` on our CI to check if no constraints were violated


# Setup & Run
--------------

Prerequsites:
------------

Windows
-------
Microsoft Visual C++ 2015 Redistributable Package should be installed prior to RavenDB launch.
https://support.microsoft.com/en-us/help/2977003/the-latest-supported-visual-c-downloads
See also: https://docs.microsoft.com/en-us/dotnet/core/windows-prerequisites


Linux/MacOS
-----------
It is recommended that you update your OS before launching an instance of RavenDB.
For example, Ubuntu-16.x as an updated OS doesn't require any additional packages.
libsodium (1.0.13 or up) must be installed prior to RavenDB launch. 
```
In Ubuntu 16.x: apt-get install libsodium-18 
In MacOS 10.12: brew install libsodium
```
You might need to also install additional packages, for example, 
```
apt-get install libunwind8 liblttng-ust0 libcurl3 libssl1.0.0 libuuid1 libkrb5 zlib1g libicu55
```

See also: https://docs.microsoft.com/en-us/dotnet/core/linux-prerequisites or 
https://docs.microsoft.com/en-us/dotnet/core/macos-prerequisites

Lauch RavenDB:
-------------
<path/to/ravendb>/Server/Raven.Server

Hello World Example:
--------------------
1. Launch RavenDB server instance as follows:
```
   <path/to/ravendb>/Server/Raven.Server --ServerUrl=http://localhost:8080
```
2. Install dotnet core sdk. See : https://www.microsoft.com/net/download and https://github.com/PowerShell/PowerShell/releases
3. Open terminal and type:

```
   mkdir HelloWorld
   cd HelloWorld
   dotnet new console 
```

4. Replace the following files content (both example files are in this page):
 - Program.cs content with the content of example.cs 
 -  HelloWorld.csproj content with the content of example.csproj 

5. Type:
```
   dotnet restore
   dotnet build
   dotnet run
```
The example program should create a database with a sample dataset and make few quiries on it. The results should be printed out to the console.

Enjoy.
