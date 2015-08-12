using Raven.Database.Server.Connections;
using Raven.Database.Server.WebApi.Attributes;
using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Web.Http;

namespace Raven.Database.TimeSeries.Controllers
{
	public class TimeSeriesChangesController : RavenTimeSeriesApiController
    {
		[HttpGet]
		[RavenRoute("ts/{timeSeriesName}/changes/config")]
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
			var globalConnectionState = TimeSeries.TransportState.For(id, this);
			var connectionState = globalConnectionState.TimeSeries;

            var cmd = GetQueryStringValue("command");
            if (Match(cmd, "disconnect"))
            {
				TimeSeries.TransportState.Disconnect(id);
            }
			else if (Match(cmd, "watch-time-series"))
			{
				connectionState.WatchAllTimeSeries();
			}
			else if (Match(cmd, "unwatch-time-series"))
			{
				connectionState.UnwatchAllTimeSeries();
			}
			else if (Match(cmd, "watch-time-series-key-change"))
			{
				connectionState.WatchKeyChange(name);
			}
			else if (Match(cmd, "unwatch-time-series-key-change"))
			{
				connectionState.UnwatchKeyChange(name);
			}
			else if (Match(cmd, "watch-bulk-operation"))
			{
				connectionState.WatchTimeSeriesBulkOperation(name);
			}
			else if (Match(cmd, "unwatch-bulk-operation"))
			{
				connectionState.UnwatchTimeSeriesBulkOperation(name);
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
		[RavenRoute("ts/{timeSeriesName}/changes/events")]
		public HttpResponseMessage GetChangesEvents()
        {
            var eventsTransport = new ChangesPushContent(this);
            eventsTransport.Headers.ContentType = new MediaTypeHeaderValue("text/event-stream");
            TimeSeries.TransportState.Register(eventsTransport);
            return new HttpResponseMessage { Content = eventsTransport };
        }
    }
}