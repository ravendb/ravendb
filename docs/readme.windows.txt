Startup instructions for RavenDB on Windows 
===========================================

* RavenDB as a Console Application
Open Powershell
Type:
    .\run.ps1

* RavenDB as Service
Open Powershell
Type:
    .\setup-as-service.ps1

The above command is going to install 'RavenDB' service on your machine. Note it requires to be run as administrator. It is going to ask whether you'd like to setup secure RavenDB server. The server is going to start on port 8080 or 443, if you have chosen to run in secure mode. 

You can view its status using the Get-Service Powershell cmdlet:

>  Get-Service -Name RavenDB

Status   Name               DisplayName
------   ----               -----------
Running  RavenDB            RavenDB

To manage service you can use Stop-Service and Start-Service cmdlets (requires administrator privileges).

* Setup
Open browser, if not opened automatically, at url printed in "Server available on: <url>"
Follow the web setup instructions at: https://ravendb.net/docs/article-page/6.2/csharp/start/installation/setup-wizard

* Upgrading to a New Version
Follow the upgrade instructions available at: https://ravendb.net/docs/article-page/6.2/csharp/start/installation/upgrading-to-new-version

