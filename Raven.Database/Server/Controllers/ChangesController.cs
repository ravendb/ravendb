using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Web.Http;
using Raven.Database.Server.Connections;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("")]
	public class ChangesController : RavenDbApiController
	{
		[HttpGet]
		[Route("changes/config")]
		[Route("databases/{databaseName}/changes/config")]
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

			var connectionState = Database.TransportState.For(id, this);
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
			else if (Match(cmd, "watch-conflicts"))
			{
				connectionState.WatchConflicts();
			}
			else if (Match(cmd, "unwatch-conflicts"))
			{
				connectionState.UnwatchConflicts();
			}
			else if (Match(cmd, "watch-sync"))
			{
				connectionState.WatchSync();
			}
			else if (Match(cmd, "unwatch-sync"))
			{
				connectionState.UnwatchSync();
			}
			else if (Match(cmd, "watch-folder"))
			{
				connectionState.WatchFolder(value);
			}
			else if (Match(cmd, "unwatch-folder"))
			{
				connectionState.UnwatchFolder(value);
			}
			else if (Match(cmd, "watch-cancellations"))
			{
				connectionState.WatchCancellations();
			}
			else if (Match(cmd, "unwatch-cancellations"))
			{
				connectionState.UnwatchCancellations();
			}
			else if (Match(cmd, "watch-config"))
			{
				connectionState.WatchConfig();
			}
			else if (Match(cmd, "unwatch-config"))
			{
				connectionState.UnwatchConfig();
			}
			else
			{
				return GetMessageWithObject(new
				{
					Error = "command argument is mandatory"
				}, HttpStatusCode.BadRequest);
			}

			return GetMessageWithObject(connectionState);
		}

		[HttpGet]
		[Route("changes/events")]
		[Route("databases/{databaseName}/changes/events")]
		public HttpResponseMessage GetChangesEvents()
		{
			var eventsTransport = new ChangesPushContent(this);
            eventsTransport.Headers.ContentType = new MediaTypeHeaderValue("text/event-stream");
			Database.TransportState.Register(eventsTransport);
			return new HttpResponseMessage {Content = eventsTransport};
		}

		private bool Match(string x, string y)
		{
			return string.Equals(x, y, StringComparison.OrdinalIgnoreCase);
		}
	}
}