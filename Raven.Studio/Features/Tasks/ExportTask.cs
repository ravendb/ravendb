using System;
using System.IO;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using Ionic.Zlib;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Connection.Async;
using Raven.Client.Silverlight.Connection;
using Raven.Client.Silverlight.Connection.Async;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Tasks
{
    public class ExportTask : TaskModel
    {
        private readonly IAsyncDatabaseCommands asyncDatabaseCommands;

        public ExportTask(IAsyncDatabaseCommands asyncDatabaseCommands)
        {
            this.asyncDatabaseCommands = asyncDatabaseCommands;
            Name = "Export Database";
            Description = "Export your database to a dump file. By default, both indexes and documents are exported.\nYou can optionally choose to export just indexes.";
        }

        public class ExportDatabaseCommand : Command
        {
            const int BatchSize = 512;

            private readonly IAsyncDatabaseCommands asyncDatabaseCommands;
            private readonly Action<string> output;
            private Stream stream;
            private GZipStream gZipStream;
            private StreamWriter streamWriter;
            private JsonTextWriter jsonWriter;

            public ExportDatabaseCommand(IAsyncDatabaseCommands asyncDatabaseCommands, Action<string> output)
            {
                this.asyncDatabaseCommands = asyncDatabaseCommands;
                this.output = output;
            }

            public override void Execute(object parameter)
            {
                var saveFile = new SaveFileDialog
                {
                    DefaultExt = ".raven.dump",
                    Filter = "Raven Dumps|*.raven.dump"
                };
                var dialogResult = saveFile.ShowDialog() ?? false;

                if (!dialogResult)
                    return;

                stream = saveFile.OpenFile();
                gZipStream = new GZipStream(stream, CompressionMode.Compress);
                streamWriter = new StreamWriter(gZipStream);
                jsonWriter = new JsonTextWriter(streamWriter)
                {
                    Formatting = Formatting.Indented
                };

                output(string.Format("Exporting to {0}", saveFile.SafeFileName));

                output("Begin reading indexes");

                jsonWriter.WriteStartObject();
                jsonWriter.WritePropertyName("Indexes");
                jsonWriter.WriteStartArray();

                ReadIndexes(0).Catch(exception => Deployment.Current.Dispatcher.InvokeAsync(() => Finish(exception)));
            }

            private Task ReadIndexes(int totalCount)
            {
                var url = ("/indexes/?start=" + totalCount + "&pageSize=" + BatchSize).NoCache();
                var request = asyncDatabaseCommands.CreateRequest(url, "GET");
                return request.ReadResponseStringAsync()
                    .ContinueOnSuccess(documents =>
                    {
                        var array = JArray.Parse(documents);
                        if (array.Count == 0)
                        {
                            output(string.Format("Done with reading indexes, total: {0}", totalCount));

                            jsonWriter.WriteEndArray();
                            jsonWriter.WritePropertyName("Docs");
                            jsonWriter.WriteStartArray();

                            return ReadDocuments(Guid.Empty, 0);
                        }
                        else
                        {
                            totalCount += array.Count;
                            output(string.Format("Reading batch of {0,3} indexes, read so far: {1,10:#,#}", array.Count,
                                                 totalCount));
                            foreach (JToken item in array)
                            {
                                item.WriteTo(jsonWriter);
                            }

                            return ReadIndexes(totalCount);
                        }
                    });
            }

            private Task ReadDocuments(Guid lastEtag, int totalCount)
            {
                var url = ("/docs/?pageSize=" + BatchSize + "&etag=" + lastEtag).NoCache();
                var request = asyncDatabaseCommands.CreateRequest(url, "GET");
                return request.ReadResponseStringAsync()
                    .ContinueOnSuccess(docs =>
                    {
                        var array = JArray.Parse(docs);
                        if (array.Count == 0)
                        {
                            output(string.Format("Done with reading documents, total: {0}", totalCount));
                            jsonWriter.WriteEndArray();
                            jsonWriter.WriteEndObject();

                            return Deployment.Current.Dispatcher.InvokeAsync(() => Finish(null));
                        }
                        else
                        {
                            totalCount += array.Count;
                            output(string.Format("Reading batch of {0,3} documents, read so far: {1,10:#,#}", array.Count,
                                   totalCount));
                            foreach (JToken item in array)
                            {
                                item.WriteTo(jsonWriter);
                            }
                            lastEtag = new Guid(array.Last.Value<JObject>("@metadata").Value<string>("@etag"));

                            return ReadDocuments(lastEtag, totalCount);
                        }
                    });


            }

            private void Finish(Exception exception)
            {
                streamWriter.Flush();
                streamWriter.Dispose();
                stream.Dispose();

                output("Export complete");
                if (exception != null)
                    output(exception.ToString());
            }
        }

        public override ICommand Action
        {
            get { return new ExportDatabaseCommand(asyncDatabaseCommands, line => Output.Execute(() => Output.Add(line))); }
        }
    }
}
