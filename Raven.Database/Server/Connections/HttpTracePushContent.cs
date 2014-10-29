using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

using Raven.Abstractions.Logging;
using Raven.Database.Server.Controllers;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions.Extensions;

namespace Raven.Database.Server.Connections
{
    public class HttpTracePushContent : HttpContent, IEventsTransport
	{
	    private const int QueueCapacity = 10000;

		private readonly ILog log = LogManager.GetCurrentClassLogger();

	    private bool hitCapacity = false;

		public string Id { get; private set; }

		public bool Connected { get; set; }

		public event Action Disconnected = delegate { };

        private readonly BlockingCollection<object> msgs = new BlockingCollection<object>(QueueCapacity);

        public HttpTracePushContent(RavenBaseApiController controller)
		{
			Connected = true;
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
                        object message;
                        while (msgs.TryTake(out message, millisecondsTimeout: 1000))
                        {
                            if (Connected == false)
                                return;

                            await SendMessage(message, writer);
                        }

                        await writer.WriteAsync("data: { \"Type\": \"Heartbeat\" }\r\n\r\n");
                        await writer.FlushAsync();
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
		}

		public void SendAsync(object msg)
		{
            if (msgs.TryAdd(msg) == false)
			{
                if (hitCapacity == false)
                {
                    hitCapacity = true;
                    log.Warn("Reached max capacity of HttpTrace queue, id = " + Id);
                }
			}
		}

        public string ResourceName {get; set; }

        public long CoolDownWithDataLossInMiliseconds { get; set; }
        
    }
}
