Raven DB
========
This release contains the following

/Server - The files required to run Raven in server / service mode.
		  Execute /Server/RavenDB.exe /install to register and start the Raven service
		  
/Web	- The files required to run Raven under IIS.
		  Create an IIS site in the /Web directory to start the Raven site.
		  
/Client	- The files required to run the Raven client, in server or embedded mode.
		  Reference the RavenClient.dll and create a DocumentStore, passing a URL
		  or a directory.
		  
For more information, start the Raven Service (by executing /server/ravendb.exe) and visit
http://localhost:8080 to read Raven's documentation.

For any questions, please visit: http://groups.google.com/group/ravendb/