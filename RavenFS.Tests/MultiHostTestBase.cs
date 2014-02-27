using System.IO;
using Raven.Client.RavenFS;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Database.Server.RavenFS;
using Raven.Server;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

namespace RavenFS.Tests
{
	public abstract class MultiHostTestBase : WithNLog, IDisposable
	{
		protected const string UrlBase = "http://localhost:";
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

            var dataPath = @"~\data\" + port;

			//HttpSelfHostConfiguration config = null;
			var configuration = new InMemoryRavenConfiguration();
			configuration.Initialize();

            configuration.DataDirectory = dataPath;
            configuration.FileSystemDataDirectory = Path.Combine(dataPath, "FileSystem");
		    configuration.Port = port;

			Task.Factory.StartNew(() => // initialize in MTA thread
				                      {
					                      IOExtensions.DeleteDirectory(configuration.DataDirectory);
										  var server = new RavenDbServer(configuration);
										  disposables.Add(server);
				                      })
			    .Wait();
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
				disposables.OfType<RavenDbServer>().First(x => x.Server.FileSystem.Configuration.Port == Ports[index]).Server.FileSystem;
		}

		protected RavenDbServer GetServer(int index)
		{
			return
				disposables.OfType<RavenDbServer>().First(x => x.Server.FileSystem.Configuration.Port == Ports[index]);
		}

		public virtual void Dispose()
		{
			foreach (var disposable in disposables)
			{
				disposable.Dispose();
			}
		}
	}
}