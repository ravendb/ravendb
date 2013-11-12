// -----------------------------------------------------------------------
//  <copyright file="EventsTransport.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Server.Abstractions;
using Raven.Database.Util;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Database.Server.Connections
{
    public class EventsTransport : IEventsTransport
    {
        private readonly ILog log = LogManager.GetCurrentClassLogger();

        private readonly IHttpContext context;

        public string Id { get; private set; }
        public bool Connected { get; set; }

        public event Action Disconnected = delegate { };

        private readonly ConcurrentQueue<object> msgs = new ConcurrentQueue<object>();
        private readonly AsyncManualResetEvent manualResetEvent = new AsyncManualResetEvent();

        public EventsTransport(IHttpContext context)
        {
            this.context = context;
            Connected = true;
            Id = context.Request.QueryString["id"];
            if (string.IsNullOrEmpty(Id))
                throw new ArgumentException("Id is mandatory");

        }

        public async Task ProcessAsync()
        {
            context.Response.ContentType = "text/event-stream";
            while (Connected)
            {
                try
                {
                    var result = await manualResetEvent.WaitAsync(5000);
                    if (Connected == false)
                        return;

                    if (result == false)
                    {
                        await context.Response.WriteAsync("data: { 'Type': 'Heartbeat' }\r\n\r\n");
                        continue;
                    }
                    manualResetEvent.Reset();
                    object msg;
                    while (msgs.TryDequeue(out msg))
                    {
                        var obj = JsonConvert.SerializeObject(msg, Formatting.None, new EtagJsonConverter());
                        await context.Response.WriteAsync("data: " + obj + "\r\n\r\n");
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
