using System;
using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("")]
	public class ChangesController : RavenApiController
	{
		[HttpGet("changes/config")]
		public HttpResponseMessage ChangeConfigGet()
		{
			var id = GetQueryStringValue("id");
			if (string.IsNullOrEmpty(id))
			{
				return GetMessageWithObject(new
				{
					Error = "id query string parameter is mandatory when using changes/config endpoint"
				}, HttpStatusCode.BadRequest);
			}

			var name = GetQueryStringValue("value");
			var connectionState = Database.TransportState.For(id);
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

			return GetMessageWithObject(connectionState);
		}

		private bool Match(string x, string y)
		{
			return string.Equals(x, y, StringComparison.OrdinalIgnoreCase);
		}
	}
}
