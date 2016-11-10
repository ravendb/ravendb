using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Json;
using Raven.Abstractions.Logging;
using Raven.Abstractions.MEF;
using Raven.Database.Extensions;
using Raven.Database.FileSystem.Actions;
using Raven.Database.FileSystem.Infrastructure;
using Raven.Database.FileSystem.Plugins;
using Raven.Database.FileSystem.Storage;
using Raven.Database.FileSystem.Util;
using Raven.Database.Plugins;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Database.Util.Streams;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Controllers
{
    public class FilesStreamsController : BaseFileSystemApiController
    {
        private static readonly ILog log = LogManager.GetCurrentClassLogger();


        [HttpGet]
        [RavenRoute("fs/{fileSystemName}/streams/files")]
        public HttpResponseMessage StreamFilesGet()
        {
            var etag = GetEtagFromQueryString();
            var pageSize = GetPageSize(int.MaxValue);

            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new PushStreamContent((stream, content, transportContext) => StreamToClient(stream, pageSize, etag, FileSystem.ReadTriggers))
                {
                    Headers = { ContentType = new MediaTypeHeaderValue("application/json") { CharSet = "utf-8" } }
                }
            };
        }

        private void StreamToClient(Stream stream, int pageSize, Etag etag, OrderedPartCollection<AbstractFileReadTrigger> readTriggers)
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
                    var returnedCount = 0;

                    while (true)
                    {
                        var files = accessor.GetFilesAfter(etag, pageSize);

                        var fileCount = 0;

                        foreach (var file in files)
                        {
                            fileCount++;

                            cts.Token.ThrowIfCancellationRequested();

                            etag = file.Etag;

                            if (readTriggers.CanReadFile(file.FullPath, file.Metadata, ReadOperation.Load) == false)
                                continue;

                            timeout.Delay();

                            var doc = RavenJObject.FromObject(file);
                            doc.WriteTo(writer);
                            writer.WriteRaw(Environment.NewLine);

                            returnedCount++;
                        }

                        if (fileCount == 0)
                            break;

                        if (returnedCount == pageSize)
                            break;
                    }
                });

                writer.WriteEndArray();
                writer.WriteEndObject();
                writer.Flush();
            }
        }

        [HttpPost]
        [RavenRoute("fs/{fileSystemName}/streams/Export")]
        public async Task<HttpResponseMessage> Export()
        {
            var filesJson = await ReadJsonAsync().ConfigureAwait(false);
            var fileNames = filesJson.Value<RavenJArray>("FileNames").Values<string>().ToArray();
            


            var pushStreamContent = new PushStreamContent((stream, content, transportContext) => StreamExportToClient( stream, fileNames))
            {
                Headers =
                {
                    
                    ContentType = new MediaTypeHeaderValue("application/json")
                    {
                        CharSet = "utf-8",
                        
                    },
                    ContentEncoding = { "gzip" }
                }
            };

            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = pushStreamContent
            };
        }

        private void StreamExportToClient(Stream stream, string[] fileNames)
        {
            using (var bufferedStream = new BufferedStream(stream))
            {
                var binaryWriter = new BinaryWriter(bufferedStream);

                var buffer = new byte[StorageConstants.MaxPageSize];
                var pageBuffer = new byte[StorageConstants.MaxPageSize];

                foreach (var name in fileNames)
                {
                    FileAndPagesInformation fileAndPages = null;
                    var cannonizedName = FileHeader.Canonize(name);

                    Storage.Batch(accessor => fileAndPages = accessor.GetFile(cannonizedName, 0, 0));
                  

                    // if we didn't find the document, we'll write "-1" to the sourceStream, signaling that 
                    if (fileAndPages.Metadata.Keys.Contains(SynchronizationConstants.RavenDeleteMarker))
                    {
                        if (log.IsDebugEnabled)
                            log.Debug("File '{0}' is not accessible to get (Raven-Delete-Marker set)", name);

                        binaryWriter.Write(-1);
                        continue;
                    }

                    var fileSize = fileAndPages.UploadedSize;
                    binaryWriter.Write(fileSize);

                    var readingStream = StorageStream.Reading(Storage, cannonizedName);
                    var bytesRead = 0;
                    do
                    {
                        bytesRead = readingStream.ReadUsingExternalTempBuffer(buffer,0,buffer.Length,pageBuffer);
                        bufferedStream.Write(buffer, 0, bytesRead);
                    } while (bytesRead > 0);
                }
            }
        }


        [HttpPut]
        [RavenRoute("fs/{fileSystemName}/streams/Import")]
        public async Task<HttpResponseMessage> Import()
        {
            using (var stream = await Request.Content.ReadAsStreamAsync().ConfigureAwait(false))
            {
                var binaryReader = new BinaryReader(stream);
                long count = 0;
                while (true)
                {
                    string name;
                    try
                    {
                        name = binaryReader.ReadString();
                    }
                    catch (EndOfStreamException)
                    {
                        break; // done
                    }

                    var metadata = RavenJObject.Parse(binaryReader.ReadString());

                    if (name.Length > SystemParameters.KeyMost)
                    {
                        if (Log.IsDebugEnabled)
                            Log.Debug("File '{0}' was not created due to illegal name length", name);
                        return GetMessageWithString(string.Format("File '{0}' was not created due to illegal name length", name), HttpStatusCode.BadRequest);
                    }

                    var contentSize = binaryReader.ReadInt64();
                    var options = new FileActions.PutOperationOptions
                    {
                        ContentSize = contentSize
                    };

                    RavenJToken lastModifiedJToken;
                    if (metadata.TryGetValue(Constants.RavenLastModified, out lastModifiedJToken))
                    {
                        DateTimeOffset lastModified;
                        if (DateTimeOffset.TryParse(lastModifiedJToken.Value<string>(), out lastModified))
                            options.LastModified = lastModified;
                    }

                    options.PreserveTimestamps = true;

                    var fileStream = new PartialStream(stream, contentSize);
                    var tcs = new TaskCompletionSource<Stream>();
                    tcs.SetResult(fileStream);

                    await FileSystem.Files.PutAsync(name, null, metadata, () => tcs.Task, options).ConfigureAwait(false);

                    if (count%100==0)
                        SynchronizationTask.Context.NotifyAboutWork();
                }

                SynchronizationTask.Context.NotifyAboutWork();


            }
                

            return new HttpResponseMessage(HttpStatusCode.OK)
            {
            };
        }

        private static void ReadFromStreamToMemoryStream(Stream sourceStream, long bytesToCopy, byte[] copyBuffer, MemoryStream destinationStream)
        {
            destinationStream.Position = 0;
            int bytesRead = 0;
            while (bytesRead< bytesToCopy)
            {
                var bytesReadInCurrentIteration = sourceStream.Read(copyBuffer, 0, (int) Math.Min(bytesToCopy - bytesRead, copyBuffer.Length));

                destinationStream.Write(copyBuffer, 0, bytesReadInCurrentIteration);

                bytesRead += bytesReadInCurrentIteration;
            }
            destinationStream.Position = 0;
        }


        [HttpGet]
        [RavenRoute("fs/{fileSystemName}/streams/query")]
        public HttpResponseMessage StreamQueryGet(string query, [FromUri] string[] sort)
        {
            var start = Paging.Start;
            var pageSize = GetPageSize(int.MaxValue);

            int _;
            long __;

            var files = Search.Query(query, sort, start, pageSize, out _, out __);

            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new PushStreamContent((stream, content, transportContext) => StreamQueryResultsToClient(stream, files, FileSystem.ReadTriggers))
                {
                    Headers = { ContentType = new MediaTypeHeaderValue("application/json") { CharSet = "utf-8" } }
                }
            };
        }

        private void StreamQueryResultsToClient(Stream stream, string[] files, OrderedPartCollection<AbstractFileReadTrigger> readTriggers)
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
                    foreach (var filename in files)
                    {
                        var fileHeader = accessor.ReadFile(filename);

                        if (fileHeader == null)
                            continue;

                        if (readTriggers.CanReadFile(fileHeader.FullPath, fileHeader.Metadata, ReadOperation.Load) == false)
                            continue;

                        timeout.Delay();
                        var doc = RavenJObject.FromObject(fileHeader);
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
