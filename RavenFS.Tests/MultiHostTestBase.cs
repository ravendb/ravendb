using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.ServiceModel;
using System.Threading.Tasks;
using System.Web.Http.SelfHost;
using Raven.Client.RavenFS;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Database.Server.RavenFS;
using Raven.Database.Server.RavenFS.Config;
using Raven.Server;

namespace RavenFS.Tests
{
	public abstract class MultiHostTestBase : WithNLog, IDisposable
	{
		protected const string UrlBase = "http://localhost.fiddler:";
		public static readonly int[] Ports = {19079, 19081};

		private readonly IList<IDisposable> disposables = new List<IDisposable>();

		protected MultiHostTestBase()
		{
			foreach (var port in Ports)
			{
			StartServerInstance(port);
			}
		}

		protected void StartServerInstance(int port)
		{
			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(port);
			//HttpSelfHostConfiguration config = null;
			var configuration = new InMemoryRavenConfiguration();
			configuration.Initialize();
			configuration.InitializeRavenFs();
			configuration.DataDirectory = "~/" + port;
			configuration.Port = port;

			Task.Factory.StartNew(() => // initialize in MTA thread
				                      {
										  //config = new HttpSelfHostConfiguration(ServerAddress(port))
										  //			 {
										  //				 MaxReceivedMessageSize = Int64.MaxValue,
										  //				 TransferMode = TransferMode.Streamed
										  //			 };
										  
										  

					                      IOExtensions.DeleteDirectory(configuration.DataDirectory);
										  var server = new RavenDbServer(configuration);
										  disposables.Add(server);
				                      })
			    .Wait();

			
			//var server = new HttpSelfHostServer(config);
			//server.OpenAsync().Wait();

		}

		protected static string ServerAddress(int port)
		{
			return UrlBase + port + "/";
		}

		protected RavenFileSystemClient NewClient(int index)
		{
			return new RavenFileSystemClient(ServerAddress(Ports[index]));
		}

		protected RavenFileSystem GetRavenFileSystem(int index)
		{
			return
				disposables.OfType<RavenDbServer>().First(
					x => x.Server.FileSystem.Configuration.DataDirectory.EndsWith(Ports[index].ToString(CultureInfo.InvariantCulture))).Server.FileSystem;
		}

		protected RavenDbServer GetServer(int index)
		{
			return
				disposables.OfType<RavenDbServer>().First(
					x => x.Server.FileSystem.Configuration.DataDirectory.EndsWith(Ports[index].ToString(CultureInfo.InvariantCulture)));

		}

		#region IDisposable Members

		public virtual void Dispose()
		{
			foreach (var disposable in disposables)
			{
				//var httpSelfHostServer = disposable as HttpSelfHostServer;
				//if (httpSelfHostServer != null)
				//	httpSelfHostServer.CloseAsync().Wait();
				disposable.Dispose();
			}
		}

		#endregion
	}
}