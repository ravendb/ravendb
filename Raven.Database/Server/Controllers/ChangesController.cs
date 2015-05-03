using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Web.Http;
using Raven.Database.Server.Connections;
using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Database.Server.Controllers
{
    [RoutePrefix("")]
	public class ChangesController : RavenDbApiController
	{
        [HttpGet]
		[RavenRoute("changes/config")]
		[RavenRoute("databases/{databaseName}/changes/config")]
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

            var name = (!String.IsNullOrEmpty(value)) ? Uri.UnescapeDataString(value) : String.Empty;
			var globalConnectionState = Database.TransportState.For(id, this);
			var connectionState = globalConnectionState.DocumentStore;

			var cmd = GetQueryStringValue("command");
            if (Match(cmd, "disconnect"))
            {
                Database.TransportState.Disconnect(id);
            }
			else if (Match(cmd, "watch-index"))
			{
				connectionState.WatchIndex(name);
			}
			else if (Match(cmd, "unwatch-index"))
			{
				connectionState.UnwatchIndex(name);
			}
			else if (Match(cmd, "watch-indexes"))
			{
				connectionState.WatchAllIndexes();
			}
			else if (Match(cmd, "unwatch-indexes"))
			{
				connectionState.UnwatchAllIndexes();
			}
            else if (Match(cmd, "watch-transformers"))
            {
				connectionState.WatchTransformers();
            } 
            else if (Match(cmd, "unwatch-transformers"))
            {
				connectionState.UnwatchTransformers();
            }
			else if (Match(cmd, "watch-doc"))
			{
				connectionState.WatchDocument(name);
			}
			else if (Match(cmd, "unwatch-doc"))
			{
				connectionState.UnwatchDocument(name);
			}
			else if (Match(cmd, "watch-docs"))
			{
				connectionState.WatchAllDocuments();
			}
			else if (Match(cmd, "unwatch-docs"))
			{
				connectionState.UnwatchAllDocuments();
			}
			else if (Match(cmd, "watch-prefix"))
			{
				connectionState.WatchDocumentPrefix(name);
			}
			else if (Equals(cmd, "unwatch-prefix"))
			{
				connectionState.UnwatchDocumentPrefix(name);
			}
			else if (Match(cmd, "watch-collection"))
			{
				connectionState.WatchDocumentInCollection(name);
			}
			else if (Equals(cmd, "unwatch-collection"))
			{
				connectionState.UnwatchDocumentInCollection(name);
			}
			else if (Match(cmd, "watch-type"))
			{
				connectionState.WatchDocumentOfType(name);
			}
			else if (Equals(cmd, "unwatch-type"))
			{
				connectionState.UnwatchDocumentOfType(name);
			}
			else if (Match(cmd, "watch-replication-conflicts"))
			{
				connectionState.WatchAllReplicationConflicts();
			}
			else if (Match(cmd, "unwatch-replication-conflicts"))
			{
				connectionState.UnwatchAllReplicationConflicts();
			}
			else if (Match(cmd, "watch-bulk-operation"))
			{
				connectionState.WatchBulkInsert(name);
			}
			else if (Match(cmd, "unwatch-bulk-operation"))
			{
				connectionState.UnwatchBulkInsert(name);
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
		[RavenRoute("changes/events")]
		[RavenRoute("databases/{databaseName}/changes/events")]
		public HttpResponseMessage GetChangesEvents()
		{
			var eventsTransport = new ChangesPushContent(this);
            eventsTransport.Headers.ContentType = new MediaTypeHeaderValue("text/event-stream");
			Database.TransportState.Register(eventsTransport);
			return new HttpResponseMessage {Content = eventsTransport};
		}
	}
}