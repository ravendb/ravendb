using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Principal;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Server.Responders;

namespace Raven.Server
{
	public class RavenDbServer : IDisposable
	{
		private readonly DocumentDatabase database;
		private readonly HttpServer server;

		public RavenDbServer(RavenConfiguration settings)
		{
			settings.LoadLoggingSettings();
			if (settings.ShouldCreateDefaultsWhenBuildingNewDatabaseFromScratch)
				settings.DatabaseCreatedFromScratch += OnDatabaseCreatedFromScratch;
			database = new DocumentDatabase(settings);
			database.SpinBackgroundWorkers();
			server = new HttpServer(settings,
			                        typeof (RequestResponder).Assembly.GetTypes()
			                        	.Where(
			                        		t => typeof (RequestResponder).IsAssignableFrom(t) && t.IsAbstract == false)

			                        	// to ensure that we would get consistent order, so we would always 
			                        	// have the responders using the same order, otherwise we get possibly
			                        	// random ordering, and that might cause issues
			                        	.OrderBy(x => x.Name)
			                        	.Select(t => (RequestResponder) Activator.CreateInstance(t))
			                        	.Select(r =>
			                        	{
			                        		r.Database = database;
			                        		r.Settings = settings;
			                        		return r;
			                        	})
				);
			server.Start();
		}

		#region IDisposable Members

		public void Dispose()
		{
			server.Dispose();
			database.Dispose();
		}

		#endregion

		private void OnDatabaseCreatedFromScratch(DocumentDatabase documentDatabase)
		{
			JArray array;
			const string name = "Raven.Server.Defaults.default.json";
			using (var defaultDocuments = GetType().Assembly.GetManifestResourceStream(name))
			{
				array = JArray.Load(new JsonTextReader(new StreamReader(defaultDocuments)));
			}

			documentDatabase.TransactionalStorage.Batch(actions =>
			{
				foreach (JObject document in array)
				{
					actions.AddDocument(
						document["DocId"].Value<string>(),
						document["Document"].Value<JObject>().ToString(),
						null,
						document["Metadata"].Value<JObject>().ToString()
						);
				}

				actions.Commit();
			});
		}

		public static void EnsureCanListenToWhenInNonAdminContext(int port)
		{
			if (CanStartHttpListener(port))
				return;

			var exit = TryGrantingHttpPrivileges(port);

			if (CanStartHttpListener(port) == false)
				Console.WriteLine("Failed to grant rights for listening to http, exit code: " + exit);
		}

		private static void GetArgsForHttpAclCmd(int port, out string args, out string cmd)
		{
			if (Environment.OSVersion.Version.Major > 5)
			{
				cmd = "netsh";
				args = string.Format(@"http add urlacl url=http://+:{0}/ user={1}", port,
				                     WindowsIdentity.GetCurrent().Name);
			}
			else
			{
				cmd = "httpcfg";
				args = string.Format("set urlacl /u http://+:{0}/ /a D:(A;;GX;;;{1})", port,
				                     WindowsIdentity.GetCurrent().User);
			}
		}

		private static bool CanStartHttpListener(int port)
		{
			try
			{
				var httpListener = new HttpListener();
				httpListener.Prefixes.Add("http://+:" + port + "/");
				httpListener.Start();
				httpListener.Stop();
				return true;
			}
			catch (HttpListenerException e)
			{
				if (e.ErrorCode != 5) //access denies
					throw;
			}
			return false;
		}

		private static int TryGrantingHttpPrivileges(int port)
		{
			string args;
			string cmd;
			GetArgsForHttpAclCmd(port, out args, out cmd);

			Console.WriteLine("Trying to grant rights for http.sys");
			try
			{
				Console.WriteLine("runas {0} {1}", cmd, args);
				var process = Process.Start(new ProcessStartInfo
				{
					Verb = "runas",
					Arguments = args,
					FileName = cmd,
				});
				process.WaitForExit();
				return process.ExitCode;
			}
			catch (Exception)
			{
				return -144;
			}
		}
	}
}