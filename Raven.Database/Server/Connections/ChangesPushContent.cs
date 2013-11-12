using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Server.Controllers;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Database.Server.Connections
{
	public class ChangesPushContent : HttpContent, IEventsTransport
	{
		private readonly ILog log = LogManager.GetCurrentClassLogger();

		public string Id { get; private set; }

		public bool Connected { get; set; }

		public event Action Disconnected = delegate { };

		private readonly ConcurrentQueue<object> msgs = new ConcurrentQueue<object>();
		private readonly AsyncManualResetEvent manualResetEvent = new AsyncManualResetEvent();

		public ChangesPushContent(RavenApiController controller)
		{
			Connected = true;
			Id = controller.GetQueryStringValue("id");
			if (string.IsNullOrEmpty(Id))
				throw new ArgumentException("Id is mandatory");
		}

		protected override async Task SerializeToStreamAsync(Stream stream, TransportContext context)
		{
			Headers.ContentType = new MediaTypeHeaderValue("text/event-stream");

			using (var writer = new StreamWriter(stream))
			{
				await writer.WriteAsync("data: { 'Type': 'Heartbeat' }\r\n\r\n");
				await writer.FlushAsync();
							
				while (Connected)
				{
					try
					{
						var result = await manualResetEvent.WaitAsync(5000);
						if (Connected == false)
							return;

						if (result == false)
						{
							await writer.WriteAsync("data: { 'Type': 'Heartbeat' }\r\n\r\n");
							await writer.FlushAsync();
							continue;
						}
						manualResetEvent.Reset();
						object message;
						while (msgs.TryDequeue(out message))
						{
							var obj = JsonConvert.SerializeObject(message, Formatting.None, new EtagJsonConverter());
							await writer.WriteAsync("data: ");
							await writer.WriteAsync(obj);
							await writer.WriteAsync("\r\n\r\n");
							await writer.FlushAsync();
						}
					}
					catch (Exception e)
					{
						Connected = false;
						log.DebugException("Error when using events transport", e);
						Disconnected();
						try
						{
							writer.WriteLine(e.ToString());
						}
						catch (Exception)
						{
							// try to send the information to the client, okay if they don't get it
							// because they might have already disconnected
						}

					}
				}
			}
		}

		protected override bool TryComputeLength(out long length)
		{
			length = 0;
			return false;
		}

		protected override void Dispose(bool disposing)
		{
			base.Dispose(disposing);
			Connected = false;
			manualResetEvent.Set();
		}

		public void SendAsync(object msg)
		{
			msgs.Enqueue(msg);
			manualResetEvent.Set();
		}
	}
}
