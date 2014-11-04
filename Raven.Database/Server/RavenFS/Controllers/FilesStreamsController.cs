using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Logging;
using Raven.Client.Connection;
using Raven.Database.Extensions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Database.Server.RavenFS.Controllers
{
    public class FilesStreamsController : RavenFsApiController
    {
        private static readonly ILog log = LogManager.GetCurrentClassLogger();


        [HttpGet]
        [Route("fs/{fileSystemName}/streams/files")]
        public HttpResponseMessage Get()
        {
            var etag = GetEtagFromQueryString();
            var pageSize = GetPageSize(FileSystem.Configuration.MaxPageSize);

            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new PushStreamContent((stream, content, transportContext) => StreamToClient(stream, pageSize, etag))
                {
                    Headers = { ContentType = new MediaTypeHeaderValue("application/json") { CharSet = "utf-8" } }
                }
            };
        }

        private void StreamToClient(Stream stream, int pageSize, Etag etag)
        {
            using (var cts = new CancellationTokenSource())
			using (var timeout = cts.TimeoutAfter(FileSystemsLandlord.SystemConfiguration.DatabaseOperationTimeout))
			using (var writer = new JsonTextWriter(new StreamWriter(stream)))
			{
			    writer.WriteStartObject();
			    writer.WritePropertyName("Results");
			    writer.WriteStartArray();

                Storage.Batch(accessor =>
                {
                    var files = accessor.GetFilesAfter(etag, pageSize);
                    foreach (var file in files)
                    {
                        timeout.Delay();
                        var doc = RavenJObject.FromObject(file);
                        doc.WriteTo(writer);

                        writer.WriteRaw(Environment.NewLine);
                    }
                });

                writer.WriteEndArray();
                writer.WriteEndObject();
                writer.Flush();
			}
        }

    }
}
