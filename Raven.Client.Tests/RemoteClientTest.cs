using System;
using System.Diagnostics;
using System.IO;
using Raven.Client.Tests.Document;
using Raven.Database.Extensions;
using Raven.Http;
using Raven.Server;
using Raven.Database;
using System.Reflection;

namespace Raven.Client.Tests
{
	public class RemoteClientTest
	{
		protected const string DbDirectory = @".\TestDb\";
		protected const string DbName = DbDirectory + @"DocDb.esb";

        protected RavenDbServer GetNewServer(int port, string path)
        {
            return new RavenDbServer(new RavenConfiguration { Port = port, DataDirectory = path, AnonymousUserAccessMode = AnonymousUserAccessMode.All });
        }

        protected RavenDbServer GetNewServerWithoutAnonymousAccess(int port, string path)
        {
            return new RavenDbServer(new RavenConfiguration { Port = port, DataDirectory = path, AnonymousUserAccessMode = AnonymousUserAccessMode.None });
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

            IOExtensions.DeleteDirectory(DbName);
            IOExtensions.DeleteDirectory(DbDirectory);
			
            Directory.CreateDirectory(DbDirectory);
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
