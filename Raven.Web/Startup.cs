using System;
using System.Diagnostics;
using Owin;
using Raven.Abstractions.Logging;
using Raven.Database.Config;
using Raven.Database.Server;

namespace Raven.Web
{
	public class Startup
	{
		private static RavenDBOptions _server;
		private static object locker = new object();
		private static readonly ILog Log = LogManager.GetCurrentClassLogger();
		private static int counter = 1;

		public void Configuration(IAppBuilder app)
		{
			if (_server == null)
			{
				lock (locker)
				{
					if (_server == null)
					{
						var p = Process.GetCurrentProcess();
						var aid = AppDomain.CurrentDomain.Id;
						bool explicitError = false;
						try
						{
							Log.Info("Startup Configuration Called {0} times, process: {1}, app  domain: {2}", counter, p.Id, aid);
							counter++;
							var sp = Stopwatch.StartNew();
							_server = new RavenDBOptions(new RavenConfiguration());
							Log.Info("Startup Configuration completed in {0} , process: {1}, app  domain: {2}", sp.ElapsedMilliseconds, p.Id, aid);
						}
						catch (Exception ex)
						{
							Log.ErrorException(string.Format("Startup Configuration Failed, process: {0}, app  domain: {1}", p.Id, aid), ex);
							explicitError = true;
							if (_server != null)
							{
								_server.Dispose();
								_server = null;
							}
							throw;
						}
						finally
						{
							if (_server == null && explicitError == false)
							{
								Log.Error("Statrup configuration completed without creating RavenDBOptions, probably aborted, process: {0}, app  domain: {1}", p.Id, aid);
							}

						}
					}
				}
			}
			app.UseRavenDB(_server);
		}
	}
}