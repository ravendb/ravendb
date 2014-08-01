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
using Raven.Json.Linq;
using Raven.Abstractions.Extensions;

namespace Raven.Database.Server.Connections
{
	public class ChangesPushContent : HttpContent, IEventsTransport
	{
		private readonly ILog log = LogManager.GetCurrentClassLogger();

		public string Id { get; private set; }

		public bool Connected { get; set; }

		public event Action Disconnected = delegate { };
        public long CoolDownWithDataLossInMiliseconds { get; set; }

        private long lastMessageSentTick = 0;
        private object lastMessageEnqueuedAndNotSent = null;

		private readonly ConcurrentQueue<object> msgs = new ConcurrentQueue<object>();
		private readonly AsyncManualResetEvent manualResetEvent = new AsyncManualResetEvent();

        public ChangesPushContent(RavenBaseApiController controller)
		{
			Connected = true;
			Id = controller.GetQueryStringValue("id");
            
			if (string.IsNullOrEmpty(Id))
				throw new ArgumentException("Id is mandatory");

            long coolDownWithDataLossInMiliseconds = 0;
			long.TryParse(controller.GetQueryStringValue("coolDownWithDataLoss"), out coolDownWithDataLossInMiliseconds);
            CoolDownWithDataLossInMiliseconds = coolDownWithDataLossInMiliseconds;
		}

		protected override async Task SerializeToStreamAsync(Stream stream, TransportContext context)
		{
			using (var writer = new StreamWriter(stream))
			{
                await writer.WriteAsync("data: { \"Type\": \"Heartbeat\" }\r\n\r\n");
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
                            await writer.WriteAsync("data: { \"Type\": \"Heartbeat\" }\r\n\r\n");
							await writer.FlushAsync();

                            if (lastMessageEnqueuedAndNotSent != null)
                            {
								await SendMessage(lastMessageEnqueuedAndNotSent, writer);
                            }
							continue;
						}

						manualResetEvent.Reset();

						object message;
						while (msgs.TryDequeue(out message))
						{
							if (CoolDownWithDataLossInMiliseconds > 0 && Environment.TickCount - lastMessageSentTick < CoolDownWithDataLossInMiliseconds)
                            {
                                lastMessageEnqueuedAndNotSent = message;
                                continue;
                            }

							await SendMessage(message, writer);
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

		private async Task SendMessage(object message, StreamWriter writer)
		{
            var o = JsonExtensions.ToJObject(message);        
            await writer.WriteAsync("data: ");
            await writer.WriteAsync(o.ToString(Formatting.None));
            await writer.WriteAsync("\r\n\r\n");
            await writer.FlushAsync();
			lastMessageEnqueuedAndNotSent = null;
			lastMessageSentTick = Environment.TickCount;
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
