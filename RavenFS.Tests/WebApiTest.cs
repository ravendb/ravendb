using System;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.RavenFS;
using Raven.Database.Config;
using Raven.Database.Server;
using Raven.Database.Server.RavenFS;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Server;

namespace RavenFS.Tests
{
	public class WebApiTest : WithNLog, IDisposable
	{
		private const string Url = "http://localhost:8079";
		protected WebClient WebClient;
		private RavenFileSystem ravenFileSystem;
		private RavenDbServer server;

		static WebApiTest()
		{
			try
			{
				new Uri("http://localhost/?query=Customer:Northwind%20AND%20Preferred:True");
			}
			catch
			{
			}
		}

		public WebApiTest()
		{
			IOExtensions.DeleteDirectory("Test");
			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(8079);
			var configuration = new InMemoryRavenConfiguration();
			configuration.Initialize();
			configuration.InitializeRavenFs();
			configuration.DataDirectory = "~/Test";
			configuration.Port = 8079;

			Task.Factory.StartNew(() => // initialize in MTA thread
									  {

										  //config = new HttpSelfHostConfiguration(Url)
										  //		   {
										  //			   MaxReceivedMessageSize = Int64.MaxValue,
										  //			   TransferMode = TransferMode.Streamed
										  //		   };
										  //var configuration = new InMemoryRavenConfiguration();
										  //configuration.InitializeRavenFs();
										  //configuration.DataDirectory = "~/Test";
										  //ravenFileSystem = new RavenFileSystem(configuration);
										  //ravenFileSystem.Start(config);
//	  ravenFileSystem.Start(config);
										
										//  ravenFileSystem = new RavenFileSystem(configuration);
									
										  server = new RavenDbServer(configuration);
									  })
				.Wait();

			


			WebClient = new WebClient
				            {
					            BaseAddress = Url
				            };
		}

		public virtual void Dispose()
		{
			server.Dispose();
		}

		protected HttpWebRequest CreateWebRequest(string url)
		{
			return (HttpWebRequest) WebRequest.Create(Url + url);
		}

		protected RavenFileSystemClient NewClient()
		{
			return new RavenFileSystemClient(Url);
		}

		protected RavenFileSystem GetRavenFileSystem()
		{
			return server.Server.FileSystem;
		}
	}
}