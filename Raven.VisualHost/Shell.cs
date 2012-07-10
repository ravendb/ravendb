using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Windows.Forms;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Document;
using Raven.Database.Config;
using Raven.Database.Server;
using Raven.Database.Server.Abstractions;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Client.Extensions;

namespace Raven.VisualHost
{
	public partial class Shell : Form
	{
		private bool ignoreHilo = true;
		readonly List<RavenDbServer> servers = new List<RavenDbServer>();
		public Shell()
		{
			InitializeComponent();
		}

		private void StartServers_Click(object sender, EventArgs e)
		{
			Reset();

			for (int i = 0; i < NumberOfServers.Value; i++)
			{
				var port = 8079 - i;
				var ravenDbServer = new RavenDbServer(new RavenConfiguration
				{
					Port = port,
					//DataDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Server-" + port, "Data"),
					RunInMemory = true,
					AnonymousUserAccessMode = AnonymousUserAccessMode.All,

				});

				var serverLog = new ServerLog
				{
					Server = ravenDbServer,
					Dock = DockStyle.Fill,
					Url = ravenDbServer.Server.Configuration.ServerUrl
				};
				ServerTabs.TabPages.Add(serverLog.Url);
				var tabPage = ServerTabs.TabPages[ServerTabs.TabPages.Count - 1];
				tabPage.Tag = serverLog;
				ravenDbServer.Server.BeforeDispatchingRequest = context =>
				{
					var adaptHttpContext = AdaptHttpContext(context, serverLog);
					if (adaptHttpContext != null)
					{
						var increment = serverLog.IncrementRequest();
						BeginInvoke((Action) (() =>
						{
							tabPage.Text = string.Format("{0} ({1})", serverLog.Url, increment);
						}));
					}

					return adaptHttpContext;
				};
				servers.Add(ravenDbServer);

				tabPage.Controls.Add(serverLog);
			}
		}

		private readonly string[] pathsToFilter = new[]
		{
			"/docs/Raven/Replication/Destinations",
			"/favicon.ico"
		};

		private Action AdaptHttpContext(IHttpContext httpContext, ServerLog serverLog)
		{
			if (pathsToFilter.Contains(httpContext.Request.Url.AbsolutePath))
				return null;
			if (ignoreHilo && httpContext.Request.Url.AbsolutePath.StartsWith("/docs/Raven/Hilo/"))
				return null;
			if (httpContext.Request.Headers["Raven-Timer-Request"] != null)
				return null;

			var requestStream = new MemoryStream();
			httpContext.SetRequestFilter(stream =>
			{
				stream.CopyTo(requestStream);
				return new MemoryStream(requestStream.ToArray());
			});
			var responseStream = new MemoryStream();
			httpContext.SetResponseFilter(stream => new MultiStreamWriter(responseStream, stream));

			var trackedRequest = new TrackedRequest
			{
				Method = httpContext.Request.HttpMethod,
				Url = httpContext.Request.RawUrl,
				RequestHeaders = new NameValueCollection(httpContext.Request.Headers),
				RequestContent = requestStream,
				ResponseContent = responseStream,
				ResponseHeaders = httpContext.Response.GetHeaders(),
			};

			return () =>
			{
				trackedRequest.Status = httpContext.Response.StatusCode;
				serverLog.AddRequest(trackedRequest);
			};
		}

		private void Reset()
		{
			foreach (var ravenDbServer in servers)
			{
				ravenDbServer.Dispose();
			}
			servers.Clear();
			ServerTabs.TabPages.Clear();
		}

		private void clearToolStripMenuItem_Click(object sender, EventArgs e)
		{
			foreach (TabPage serverLog in ServerTabs.TabPages)
			{
				ServerLog log = ((ServerLog)serverLog.Tag);
				log.Clear();
				serverLog.Text = log.Url;
			}
		}

		private void ignoreHiloToolStripMenuItem_Click(object sender, EventArgs e)
		{
			((ToolStripMenuItem) sender).Checked = ignoreHilo = !ignoreHilo;
		}

		private void setupMasterMasterReplicationToolStripMenuItem_Click(object sender, EventArgs e)
		{
			foreach (var ravenDbServer in servers)
			{
				var replicationServer = servers.Where(s => s != ravenDbServer);

				var doc = new RavenJObject
				{
					{
						"Destinations", new RavenJArray(replicationServer.Select(s => new RavenJObject
						{
							{"Url", s.Database.Configuration.ServerUrl}
						}))
						}
				};
				ravenDbServer.Database.Put("Raven/Replication/Destinations", null, doc, new RavenJObject(), null);
			}

			MessageBox.Show("Setup replication between all servers (Master/Master)");

		}

		private void setupSlaveMasterReplicationToolStripMenuItem_Click(object sender, EventArgs e)
		{
			var ravenDbServer = servers.FirstOrDefault();
			if (ravenDbServer == null)
				return;

			var replicationServer = servers.Where(s => s != ravenDbServer);

			var doc = new RavenJObject
				{
					{
						"Destinations", new RavenJArray(replicationServer.Select(s => new RavenJObject
						{
							{"Url", s.Database.Configuration.ServerUrl}
						}))
						}
				};
			ravenDbServer.Database.Put("Raven/Replication/Destinations", null, doc, new RavenJObject(), null);

			MessageBox.Show("Setup replication between all servers (Master/Slave)");
		}

		private void setupDatbasesToolStripMenuItem_Click(object sender, EventArgs e)
		{
			foreach (var ravenDbServer in servers)
			{
				using(var docStore = new DocumentStore
				{
					Url = ravenDbServer.Server.Configuration.ServerUrl,
					DefaultDatabase = "Users"
				}.Initialize())
				{
					docStore.DatabaseCommands.EnsureDatabaseExists("Users");
					docStore.DatabaseCommands.EnsureDatabaseExists("Questions");

					var replicationServer = servers.Where(s => s != ravenDbServer);

					var doc = new RavenJObject
					{
						{
							"Destinations", new RavenJArray(replicationServer.Select(s => new RavenJObject
							{
								{"Url", s.Database.Configuration.ServerUrl + "databases/Users"}
							}))
						}
					};
					docStore.DatabaseCommands.Put("Raven/Replication/Destinations", null, doc, new RavenJObject());
				}
			}
		}

		private void sToolStripMenuItem_Click(object sender, EventArgs e)
		{
			var stoppedWorkers = ((ToolStripMenuItem)sender).Checked;
			foreach (var ravenDbServer in servers)
			{
				ravenDbServer.Server.ForAllDatabases(database =>
				{
					if (stoppedWorkers)
						database.SpinBackgroundWorkers();
					else
						database.StopBackgroundWorkers();
				});
			}
			((ToolStripMenuItem) sender).Checked = !stoppedWorkers;
		}
	}
}
