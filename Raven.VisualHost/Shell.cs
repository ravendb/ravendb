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
using Raven.Database.Config;
using Raven.Database.Server.Abstractions;
using Raven.Server;

namespace Raven.VisualHost
{
	public partial class Shell : Form
	{
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
				var ravenDbServer = new RavenDbServer(new RavenConfiguration
				{
					Port = 8079 - i,
					RunInMemory = true
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
						var increment = Interlocked.Increment(ref serverLog.NumOfRequests);
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

		private static Action AdaptHttpContext(IHttpContext httpContext, ServerLog serverLog)
		{
			if (httpContext.Request.Url.AbsolutePath == "/favicon.ico")
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
	}
}
