using Raven.Database.Server.Connections;
using Raven.Database.Server.WebApi.Attributes;
using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Web.Http;

namespace Raven.Database.Counters.Controllers
{
	public class CountersChangesController : BaseCountersApiController
    {
		[HttpGet]
		[RavenRoute("cs/{counterStorageName}/changes/config")]
        public HttpResponseMessage GetChangeConfig()
        {
            var value = GetQueryStringValue("value");
            var id = GetQueryStringValue("id");
            if (string.IsNullOrEmpty(id))
            {
                return GetMessageWithObject(new
                {
                    Error = "id query string parameter is mandatory when using changes/config endpoint"
                }, HttpStatusCode.BadRequest);
            }

            var name = (!string.IsNullOrEmpty(value)) ? Uri.UnescapeDataString(value) : string.Empty;
			var globalConnectionState = CounterStorage.TransportState.For(id, this);
			var connectionState = globalConnectionState.CounterStorage;

            var cmd = GetQueryStringValue("command");
            if (Match(cmd, "disconnect"))
            {
				CounterStorage.TransportState.Disconnect(id);
            }
			else if (Match(cmd, "watch-counters"))
			{
				connectionState.WatchAllCounters();
			}
			else if (Match(cmd, "unwatch-counters"))
			{
				connectionState.UnwatchAllCounters();
			}
			else if (Match(cmd, "watch-counter-change"))
			{
				connectionState.WatchChange(name);
			}
			else if (Match(cmd, "unwatch-counter-change"))
			{
				connectionState.UnwatchChange(name);
			}
			else if (Match(cmd, "watch-counters-prefix"))
			{
				connectionState.WatchPrefix(name);
			}
			else if (Match(cmd, "unwatch-counters-prefix"))
			{
				connectionState.UnwatchPrefix(name);
			}
			else if (Match(cmd, "watch-counters-in-group"))
            {
				connectionState.WatchCountersInGroup(name);
            }
			else if (Match(cmd, "unwatch-counters-in-group"))
            {
				connectionState.UnwatchCountersInGroup(name);
            }
			else if (Match(cmd, "watch-bulk-operation"))
			{
				connectionState.WatchCounterBulkOperation(name);
			}
			else if (Match(cmd, "unwatch-bulk-operation"))
			{
				connectionState.UnwatchCounterBulkOperation(name);
			}
            else
            {
                return GetMessageWithObject(new
                {
                    Error = "command argument is mandatory"
                }, HttpStatusCode.BadRequest);
            }

            return GetMessageWithObject(globalConnectionState);
        }

        [HttpGet]
		[RavenRoute("cs/{counterStorageName}/changes/events")]
		public HttpResponseMessage GetChangesEvents()
        {
            var eventsTransport = new ChangesPushContent(this);
            eventsTransport.Headers.ContentType = new MediaTypeHeaderValue("text/event-stream");
			CounterStorage.TransportState.Register(eventsTransport);
            return new HttpResponseMessage { Content = eventsTransport };
        }
    }
}