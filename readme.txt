# Raven DB

## A 2nd generation document database

Issue tracker: http://issues.hibernatingrhinos.com

You can start the Raven Service by executing /server/raven.server.exe, and then you can then visit
http://localhost:8080 for looking at the UI.

For any questions, please visit: http://groups.google.com/group/ravendb/

RavenDB's homepage: http://ravendb.net

For your convenience RavenDB is also available as nuget packages: RavenDB.Client and RavenDB.Embedded.

## This release contains the following:

/Client		- RavenDB lightweight client for .NET 4.0 and its dependencies.
		*** This is the recommended client to use ***


/Client-3.5	- RavenDB lightweight client for .NET 3.5 and its dependencies.


/Silverlight	- A lightweight Silverlight 4.0 client for RavenDB and its dependencies..


/EmbeddedClient	- The files required to run the RavenDB client, in server or embedded mode.
		  Reference Raven.Client.Embedded.dll and create a DocumentStore, passing a URL
		  or a directory. See the docs for more help.

Whichever client version you choose to use, reference all the assemblies in the corresponding folder to your project.

As for the rest of the folders in the package, here's a brief description of what they contain:

/Server		- The files required to run RavenDB in server / service mode.
		  Execute /Server/Raven.Server.exe /install to register and start the Raven service
		  
/Web		- The files required to run RavenDB under IIS.
		  Create an IIS site in the /Web directory to start the Raven site.		

/Bundles	- Bundles for extending RavenDB in various ways
	
/Samples	- Some sample applications for RavenDB
		* Under each sample application folder there is a "Start Raven.cmd" file which will
		starts Raven with all the data and indexes required to run the sample successfully.

/Backup - [Standalone backup tool](http://ravendb.net/docs/server/administration/backup-restore), for performing backup operations using a user with admin privileges.

/Smuggler - [The Import/Export utility](http://ravendb.net/docs/server/administration/export-import) for RavenDB.