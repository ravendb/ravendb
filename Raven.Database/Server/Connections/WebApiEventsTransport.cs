using System;
using System.Collections.Concurrent;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Server.Controllers;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Database.Server.Connections
{
	public class WebApiEventsTransport : IEventsTransport
	{
		private readonly RavenApiController controller;
		private readonly ILog log = LogManager.GetCurrentClassLogger();

        public string Id { get; private set; }
        public bool Connected { get; set; }

        public event Action Disconnected = delegate { };

        private readonly ConcurrentQueue<object> msgs = new ConcurrentQueue<object>();
        private readonly AsyncManualResetEvent manualResetEvent = new AsyncManualResetEvent();

		public WebApiEventsTransport(RavenApiController controller)
        {
			this.controller = controller;
			Connected = true;
            Id = controller.GetQueryStringValue("id");
            if (string.IsNullOrEmpty(Id))
                throw new ArgumentException("Id is mandatory");
        }

        public async Task ProcessAsync(HttpResponseMessage msg)
        {
			msg.Content.Headers.ContentType = new MediaTypeHeaderValue("text/event-stream");
            while (Connected)
            {
                try
                {
                    var result = await manualResetEvent.WaitAsync(5000);
                    if (Connected == false)
                        return;

                    if (result == false)
                    {
	                    var text = "data: { 'Type': 'Heartbeat' }\r\n\r\n";
						msg.Content = new StringContent(text);
						msg.Content.Headers.ContentType = new MediaTypeHeaderValue("text/event-stream");

						// await context.Response.WriteAsync();
                        continue;
                    }
                    manualResetEvent.Reset();
                    object message;
                    while (msgs.TryDequeue(out message))
                    {
						var obj = JsonConvert.SerializeObject(message, Formatting.None, new EtagJsonConverter());
						var text = "data: " + obj + "\r\n\r\n";
						msg.Content = new StringContent(text);
						msg.Content.Headers.ContentType = new MediaTypeHeaderValue("text/event-stream");
                       // await context.Response.WriteAsync("data: " + obj + "\r\n\r\n");
                    }
                }
                catch (Exception e)
                {
                    Connected = false;
                    log.DebugException("Error when using events transport", e);
                    Disconnected();
                }
            }
        }

        public void Dispose()
        {
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
