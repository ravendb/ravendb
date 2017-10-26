# RavenDB: The Premier NoSQL database for .NET

This repository contains source code for the [RavenDB](https://ravendb.net/) document database. With a RavenDB database you can set up a NoSQL data architecture, or add a NoSQL layer to your current relational database. 

![RavenDb Studio](docs/readmeScreenshot.png)

Requirements
--------------

- .NET Core 1.0
- TypeScript 2.1
- WiX Toolset 3.7 or higher

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
