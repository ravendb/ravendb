// -----------------------------------------------------------------------
//  <copyright file="ServerRunner.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Tests.Server.Runner
{
	using System;
	using System.Collections.Generic;
	using System.Linq;
	using System.Net;
	using System.Threading.Tasks;

	using Raven.Database.Config;
	using Raven.Database.Extensions;
	using Raven.Database.Server;
	using Raven.Database.Server.Abstractions;
	using Raven.Database.Util.Streams;

	using Raven.Tests.Server.Runner.Responders;

	public class ServerRunner
	{
		private readonly HttpListener listener;

		private readonly IList<ResponderBase> responders;

		private readonly IBufferPool bufferPool = new BufferPool(1024, 1024);

	    private readonly int port;

		public ServerRunner(int port)
		{
		    this.port = port;
			responders = new List<ResponderBase>
						 {
							 new ServerInstanceResponder()
						 };

			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(port);

			listener = new HttpListener();
			listener.Prefixes.Add(string.Format("http://+:{0}/", port));

			Task.Factory.StartNew(() =>
			{
				Console.WriteLine("Started...");

				while (listener.IsListening)
				{
					HttpListenerContext context = null;
					try
					{
						Console.WriteLine("Listening...");
						context = listener.GetContext();
					}
					catch (ObjectDisposedException)
					{
						break;
					}
					catch (Exception e)
					{
						Console.WriteLine("Error: " + e.StackTrace);
						continue;
					}

					Console.WriteLine("Processing...");
					ProcessRequest(context);
				}

				Console.WriteLine("Listening: " + listener.IsListening);
			}, TaskCreationOptions.LongRunning);
		}

		public bool IsRunning
		{
			get
			{
				return listener.IsListening;
			}
		}

		private void ProcessRequest(HttpListenerContext context)
		{
			IHttpContext ctx = new HttpListenerContextAdpater(context, new InMemoryRavenConfiguration
			{
				VirtualDirectory = "/"
			}, bufferPool);

			try
			{
				foreach (var responder in responders.Where(x => x.WillRespond(ctx)))
				{
					responder.Respond(ctx);
					return;
				}

				ctx.SetStatusToBadRequest();
			}
			catch (Exception)
			{
				ctx.SetStatusToBadRequest();
			}
			finally
			{
				ctx.FinalizeResponse();
			}
		}

		public void Start()
		{
			listener.Start();
            Console.WriteLine("Started listener at port: " + port);
		}
	}
}