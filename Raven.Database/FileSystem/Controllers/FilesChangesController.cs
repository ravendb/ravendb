using Raven.Abstractions.Logging;
using Raven.Database.Server.Connections;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http;

using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Database.FileSystem.Controllers
{
    public class FilesChangesController : RavenFsApiController
    {
        private static readonly ILog log = LogManager.GetCurrentClassLogger();

        [HttpGet]
        [RavenRoute("fs/{fileSystemName}/changes/config")]
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

            var connectionState = this.FileSystem.TransportState.For(id, this);

            var cmd = GetQueryStringValue("command");
            if (Match(cmd, "disconnect"))
            {
                FileSystem.TransportState.Disconnect(id);
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
        [RavenRoute("fs/{fileSystemName}/changes/events")]
        public HttpResponseMessage GetChangesEvents()
        {
            var eventsTransport = new ChangesPushContent(this);
            eventsTransport.Headers.ContentType = new MediaTypeHeaderValue("text/event-stream");
            FileSystem.TransportState.Register(eventsTransport);
            return new HttpResponseMessage { Content = eventsTransport };
        }

        private bool Match(string x, string y)
        {
            return string.Equals(x, y, StringComparison.OrdinalIgnoreCase);
        }
    }
}
