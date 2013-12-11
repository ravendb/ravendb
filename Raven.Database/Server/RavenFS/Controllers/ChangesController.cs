using System;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using Raven.Database.Server.RavenFS.Infrastructure.Connections;

namespace Raven.Database.Server.RavenFS.Controllers
{
	public class ChangesController : RavenFsApiController
	{
		[HttpGet]
		[Route("ravenfs/changes/events")]
		public HttpResponseMessage Events(string id)
		{
			var eventsTransport = new EventsTransport(id);
			RavenFileSystem.TransportState.Register(eventsTransport);

			return eventsTransport.GetResponse();
		}

		[HttpGet]
		[Route("ravenfs/changes/config")]
		public HttpResponseMessage Config(string id, string command, string value = "")
		{
			if (string.IsNullOrEmpty(id))
				throw BadRequestException("id query string parameter is mandatory when using changes/config endpoint");

			var connectionState = RavenFileSystem.TransportState.For(id);

			if (Match(command, "disconnect"))
			{
				RavenFileSystem.TransportState.Disconnect(id);
			}
			else if (Match(command, "watch-config"))
			{
				connectionState.WatchConfig();
			}
			else if (Match(command, "unwatch-config"))
			{
				connectionState.UnwatchConfig();
			}
			else if (Match(command, "watch-conflicts"))
			{
				connectionState.WatchConflicts();
			}
			else if (Match(command, "unwatch-conflicts"))
			{
				connectionState.UnwatchConflicts();
			}
			else if (Match(command, "watch-sync"))
			{
				connectionState.WatchSync();
			}
			else if (Match(command, "unwatch-sync"))
			{
				connectionState.UnwatchSync();
			}
			else if (Match(command, "watch-folder"))
			{
				connectionState.WatchFolder(value);
			}
			else if (Match(command, "unwatch-folder"))
			{
				connectionState.UnwatchFolder(value);
			}
			else if (Match(command, "watch-cancellations"))
			{
				connectionState.WatchCancellations();
			}
			else if (Match(command, "unwatch-cancellations"))
			{
				connectionState.UnwatchCancellations();
			}
			else
			{
				throw BadRequestException("command argument is mandatory");
			}

			return new HttpResponseMessage(HttpStatusCode.NoContent);
		}

		private bool Match(string x, string y)
		{
			return string.Equals(x, y, StringComparison.InvariantCultureIgnoreCase);
		}
	}
}