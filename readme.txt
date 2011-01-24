Raven DB
========
This release contains the following:

/Server - The files required to run Raven in server / service mode.
		  Execute /Server/Raven.Server.exe /install to register and start the Raven service
		  
/Web	- The files required to run Raven under IIS.
		  Create an IIS site in the /Web directory to start the Raven site.

/Client-3.5
		- The files required to run the Raven client under .NET 3.5
		
/Client
		- The files required to run the Raven client under .NET 4.0
		*** This is the recommended client to use ***

/ClientEmbedded
		- The files required to run the Raven client, in server or embedded mode.
		  Reference the RavenClient.dll and create a DocumentStore, passing a URL
		  or a directory.

/Bundles
	- Bundles that extend Raven in various ways
	
/Samples
	- The sample applications for Raven
	* Under each sample application folder there is a "Start Raven.cmd" file which will
	  starts Raven with all the data and indexes required to run the sample successfully.
	
/Raven.Smuggler.exe
	- The Import/Export utility for Raven
		  
You can start the Raven Service by executing /server/raven.server.exe, you can then visit
http://localhost:8080 for looking at the UI.

For any questions, please visit: http://groups.google.com/group/ravendb/

Raven's homepage: http://ravendb.net