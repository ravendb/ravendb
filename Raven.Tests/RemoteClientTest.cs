//-----------------------------------------------------------------------
// <copyright file="RemoteClientTest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using Raven.Client.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Http;
using Raven.Server;
using System.Reflection;
using Raven.Tests.Document;

namespace Raven.Tests
{
	public class RemoteClientTest
	{
		protected const string DbDirectory = @".\TestDb\";
		protected const string DbName = DbDirectory + @"DocDb.esb";

        protected RavenDbServer GetNewServer()
        {
        	var ravenDbServer = new RavenDbServer(new RavenConfiguration
        	{
        		Port = 8080,
        		RunInMemory = true,
        		DataDirectory = "Data",
        		AnonymousUserAccessMode = AnonymousUserAccessMode.All
        	});

			using (var documentStore = new DocumentStore
			{
				Url = "http://localhost:8080"
			}.Initialize())
			{
				new RavenDocumentsByEntityName().Execute(documentStore);
			}

        	return ravenDbServer;
        }

		protected RavenDbServer GetNewServer(int port, string path)
		{
			var ravenDbServer = new RavenDbServer(new RavenConfiguration
			{
				Port = port, 
				DataDirectory = path, 
				RunInMemory = true,
				AnonymousUserAccessMode = AnonymousUserAccessMode.All
			});

			using (var documentStore = new DocumentStore
			{
				Url = "http://localhost:" + port
			}.Initialize())
			{
				new RavenDocumentsByEntityName().Execute(documentStore);
			}
			return ravenDbServer;
		}

		protected RavenDbServer GetNewServerWithoutAnonymousAccess(int port, string path)
		{
			RavenDbServer newServerWithoutAnonymousAccess = new RavenDbServer(new RavenConfiguration { Port = port, DataDirectory = path, AnonymousUserAccessMode = AnonymousUserAccessMode.None });
			using (var documentStore = new DocumentStore
			{
				Url = "http://localhost:" + port
			}.Initialize())
			{
				new RavenDocumentsByEntityName().Execute(documentStore);
			}
			return newServerWithoutAnonymousAccess;
		}

		protected string GetPath(string subFolderName)
        {
            string retPath = Path.GetDirectoryName(Assembly.GetAssembly(typeof(DocumentStoreServerTests)).CodeBase);
            return Path.Combine(retPath, subFolderName).Substring(6); //remove leading file://
        }
        
		public RemoteClientTest()
		{
			try
			{
				new Uri("http://fail/first/time?only=%2bplus");
			}
			catch (Exception)
			{
			}

            ClearDatabaseDirectory();

			Directory.CreateDirectory(DbDirectory);
		}

		protected void ClearDatabaseDirectory()
		{
			IOExtensions.DeleteDirectory(DbName);
			IOExtensions.DeleteDirectory(DbDirectory);
		}

		public double Timer(Action action)
		{
			var startTime = DateTime.Now;
			action.Invoke();
			var timeTaken = DateTime.Now.Subtract(startTime);
			Console.WriteLine("Time take (ms)- " + timeTaken.TotalMilliseconds);
			return timeTaken.TotalMilliseconds;
		}
	}
}
