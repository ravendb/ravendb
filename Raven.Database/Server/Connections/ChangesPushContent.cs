using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
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

	    public bool Connected
	    {
            get { return connected && cancellationTokenSource.IsCancellationRequested == false; }
	        set { connected = value; }
	    }

	    private readonly DateTime _started = SystemTime.UtcNow;
		public TimeSpan Age { get { return SystemTime.UtcNow - _started; } }

		public event Action Disconnected = delegate { };
        public long CoolDownWithDataLossInMiliseconds { get; set; }

        private long lastMessageSentTick = 0;
        private object lastMessageEnqueuedAndNotSent = null;

		private readonly ConcurrentQueue<object> msgs = new ConcurrentQueue<object>();
		private readonly AsyncManualResetEvent manualResetEvent = new AsyncManualResetEvent();
	    private readonly CancellationTokenSource cancellationTokenSource;
	    private bool connected;
	    public string ResourceName { get; set; }

        public ChangesPushContent(RavenBaseApiController controller)
		{
			Connected = true;
            ResourceName = controller.ResourceName;
			Id = controller.GetQueryStringValue("id");
            
			if (string.IsNullOrEmpty(Id))
				throw new ArgumentException("Id is mandatory");
            cancellationTokenSource = WebSocketTransportFactory.RavenGcCancellation;
            long coolDownWithDataLossInMiliseconds = 0;
			long.TryParse(controller.GetQueryStringValue("coolDownWithDataLoss"), out coolDownWithDataLossInMiliseconds);
            CoolDownWithDataLossInMiliseconds = coolDownWithDataLossInMiliseconds;
		}

		protected override async Task SerializeToStreamAsync(Stream stream, TransportContext context)
		{
		    try
		    {
                using (var writer = new StreamWriter(stream))
                {
                    await writer.WriteAsync("data: { \"Type\": \"Heartbeat\" }\r\n\r\n").ConfigureAwait(false);
                    await writer.FlushAsync().ConfigureAwait(false);

                    while (Connected)
                    {
                        try
                        {
                            var result = await manualResetEvent.WaitAsync(5000, cancellationTokenSource.Token).ConfigureAwait(false);
                            if (Connected == false)
                                return;

                            if (result == false)
                            {
                                await writer.WriteAsync("data: { \"Type\": \"Heartbeat\" }\r\n\r\n").ConfigureAwait(false);
                                await writer.FlushAsync().ConfigureAwait(false);

                                if (lastMessageEnqueuedAndNotSent != null)
                                {
                                    await SendMessage(lastMessageEnqueuedAndNotSent, writer).ConfigureAwait(false);
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

                                await SendMessage(message, writer).ConfigureAwait(false);
                            }
                        }
                        catch (Exception e)
                        {
                            Connected = false;
							if (log.IsDebugEnabled)
								log.DebugException("Error when using events transport", e);
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
		    finally
		    {
                Disconnected();
		    }
		}

		private async Task SendMessage(object message, StreamWriter writer)
		{
            var o = JsonExtensions.ToJObject(message);        
            await writer.WriteAsync("data: ").ConfigureAwait(false);
            await writer.WriteAsync(o.ToString(Formatting.None)).ConfigureAwait(false);
            await writer.WriteAsync("\r\n\r\n").ConfigureAwait(false);
            await writer.FlushAsync().ConfigureAwait(false);
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
