using System;
using System.Net;
using System.ServiceModel;
using System.Threading.Tasks;
using System.Web.Http.SelfHost;
using Raven.Client.RavenFS;
using Raven.Database.Server;
using Raven.Database.Server.RavenFS;
using Raven.Database.Server.RavenFS.Config;
using Raven.Database.Server.RavenFS.Extensions;

namespace RavenFS.Tests
{
	public class WebApiTest : WithNLog, IDisposable
	{
		private const string Url = "http://localhost:19079";
		private readonly HttpSelfHostServer server;
		protected WebClient WebClient;
		private HttpSelfHostConfiguration config;
		private RavenFileSystem ravenFileSystem;

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
			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(19079);
			Task.Factory.StartNew(() => // initialize in MTA thread
				                      {
					                      config = new HttpSelfHostConfiguration(Url)
						                               {
							                               MaxReceivedMessageSize = Int64.MaxValue,
							                               TransferMode = TransferMode.Streamed
						                               };
					                      var configuration = new InMemoryConfiguration();
					                      configuration.Initialize();
					                      configuration.DataDirectory = "~/Test";
					                      ravenFileSystem = new RavenFileSystem(configuration);
					                      ravenFileSystem.Start(config);
				                      })
			    .Wait();

			server = new HttpSelfHostServer(config);
			server.OpenAsync().Wait();

			WebClient = new WebClient
				            {
					            BaseAddress = Url
				            };
		}

		public virtual void Dispose()
		{
			server.CloseAsync().Wait();
			server.Dispose();
			ravenFileSystem.Dispose();
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
			return ravenFileSystem;
		}
	}
}