# RavenDB - An ACID NoSQL Document Database
This repository contains source code for the [RavenDB](https://ravendb.net/) document database. With a RavenDB database you can set up a NoSQL data architecture or add a NoSQL layer to your current relational database.

![RavenDb Studio](docs/readmeScreenshot.png)

## Supported Platforms
- Windows
- Linux
- Docker
- MacOS
- Raspberry Pi

## Grab Your License and Latest Version
**Download the latest version of [RavenDB](https://ravendb.net/downloads#server/dev)**

## Getting Started
Install and [set up your database](https://ravendb.net/docs/article-page/latest/csharp/start/getting-started).

## Learn RavenDB Quickly
[RavenDB Bootcamp](https://ravendb.net/learn) is a free, self-directed learning course. In just three units you will learn how to use RavenDB to create fully-functional, real-world programs with NoSQL Databases. If you are unfamiliar with NoSQL, it’s okay. We will provide you with all the information you need.

## Stay Updated on New Developments
We are always adding new features to improve your RavenDB experience. Check out [our latest improvements](https://ravendb.net/docs/article-page/latest/csharp/start/whats-new), updated weekly.

## Documentation
Access [full documentation](https://ravendb.net/docs/article-page/latest/csharp) for RavenDB. Like our database, it is easy to use.

## Where to Ask for Help
If you have any questions, or need further assistance, you can [contact us directly](https://ravendb.net/contact).

## Report an Issue
You can create issues and track them at our [YouTrack](http://issues.hibernatingrhinos.com/) page.

## RavenDB Developer Community Group
If you have any questions please visit our [community group](http://groups.google.com/group/ravendb/). The solutions for the most common challenges are available. You are welcome to join!

## Submit a Pull Request
Each Pull Request will be checked against the following rules:

- `cla/signed` - all commit authors need to sign a CLA. This can be done using our [CLA sign form](http://ravendb.net/contributors/cla/sign).
- `commit/whitespace` - all changed files cannot contain TABs inside them. Before doing any work we suggest executing our `git_setup.cmd`. This will install a git pre-commit hook that will normalize all whitespaces during commits.
- `commit/message/conventions` - all commit messages (except in merge commits) must contain an issue number from our [YouTrack](http://issues.hibernatingrhinos.com) e.g. 'RavenDB-1234 Fixed issue with something'
- `tests` - this executes `build.cmd Test` on our CI to check if no constraints were violated

 <br><br>

## Setup & Run
### Prerequisites:

#### Windows
Microsoft Visual C++ 2015 Redistributable Package should be installed prior to RavenDB launch.
[Visual C++ Downloads](https://support.microsoft.com/en-us/help/2977003/the-latest-supported-visual-c-downloads)
See also: [Windows Prerequisites](https://docs.microsoft.com/en-us/dotnet/core/windows-prerequisites)

#### Linux/MacOS
It is recommended that you update your OS before launching an instance of RavenDB.
For example, Ubuntu-16.x as an updated OS doesn't require any additional packages.
libsodium (1.0.13 or up) must be installed prior to RavenDB launch.
```
In Ubuntu 16.x: apt-get install libsodium18
In MacOS 10.12: brew install libsodium
```
You might need to also install additional packages, for example:
```
apt-get install libunwind8 liblttng-ust0 libcurl3 libssl1.0.0 libuuid1 libkrb5-3 zlib1g libicu55
```

See also: [Linux Prerequisites](https://docs.microsoft.com/en-us/dotnet/core/linux-prerequisites) or [MacOS Prerequisites](https://docs.microsoft.com/en-us/dotnet/core/macos-prerequisites)

### Lauch RavenDB:
Running locally:
```
<path/to/ravendb>/Server/Raven.Server
```

Registering as service in Windows:
```
.\rvn.exe windows-service register --service-name RavenDB4
```

Running as service in Linux, add to your daemon script:
```
<path/to/ravendb>/Server/Raven.Server --daemon
```

### Hello World (.NET)

#### Server Side

- Launch a RavenDB server instance as follows:
```
   <path/to/ravendb>/Server/Raven.Server --ServerUrl=http://localhost:8080
```

- Open a web browser and enter `http://localhost:8080`
- Click on `Databases` on the left menu, and then create a new database named `SampleDataDB`
- Click on `Settings` and then on `Create Sample Data` in the left menu. Now Click on `Create`

#### Client Side

- Install dotnet core sdk. See : [Downloads](https://www.microsoft.com/net/download) and [PowerShell](https://github.com/PowerShell/PowerShell/releases)

- Open a terminal and type:

```
   mkdir HelloWorld
   cd HelloWorld
   dotnet new console
```

- Add the RavenDB Client package:

```
   dotnet add package RavenDB.Client --version 4.0.0-*
```

- Replace the content of Program.cs with the following:
```
using System;
using Raven.Client.Documents;

namespace HelloWorld
{
    class Shippers
    {
        public string Name;
        public string Phone;
    }
    
    class Program
    {
        static void Main(string[] args)
        {
            using (var store = new DocumentStore
            {
                Urls = new string[] {"http://localhost:8080"},
                Database = "SampleDataDB"
            })
            {
                store.Initialize();

                using (var session = store.OpenSession())
                {
                    var shipper = session.Load<Shippers>("shippers/1-A");
                    Console.WriteLine("Shipper #1 : " + shipper.Name + ", Phone: " + shipper.Phone);
                }
            }
        }
    }
}
```

- Type:
```
   dotnet restore
   dotnet build
   dotnet run
```

###### Enjoy :)
