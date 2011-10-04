using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Controls;
using System.Windows.Input;
using Ionic.Zlib;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Indexing;
using Raven.Client.Connection.Async;
using Raven.Json.Linq;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Tasks
{
    public class ImportTask : TaskModel
    {
        private readonly IAsyncDatabaseCommands asyncDatabaseCommands;

        public ImportTask(IAsyncDatabaseCommands asyncDatabaseCommands)
        {
            this.asyncDatabaseCommands = asyncDatabaseCommands;
            Name = "Import Database";
            Description = "Import a database from a dump file.\nImporting will overwrite any existing indexes.";       
        }

        public class ImportDatabaseCommand : Command
        {
            const int BatchSize = 512;

            private readonly IAsyncDatabaseCommands asyncDatabaseCommands;
            private readonly Action<string> output;
            private int totalCount;
            private int totalIndexes;

            public ImportDatabaseCommand(IAsyncDatabaseCommands asyncDatabaseCommands,  Action<string> output)
            {
                this.asyncDatabaseCommands = asyncDatabaseCommands;
                this.output = output;
            }

            public override void Execute(object parameter)
            {
                var openFile = new OpenFileDialog
                {
                    Filter = "Raven Dumps|*.raven.dump"
                };

                var dialogResult = openFile.ShowDialog() ?? false;

                if (!dialogResult)
                    return;

                totalCount = 0;
                totalIndexes = 0;
                
                output(string.Format("Importing from {0}", openFile.File.Name));

                var sw = Stopwatch.StartNew();

                var stream = openFile.File.OpenRead();
                JsonTextReader jsonReader;
                if (TryGetJsonReader(stream, out jsonReader) == false)
                {
                    stream.Dispose();
                    return;
                }

                if (jsonReader.TokenType != JsonToken.StartObject)
                    throw new InvalidOperationException("StartObject was expected");

                // should read indexes now
                if (jsonReader.Read() == false)
                {
                    output("Invalid Json file specified!");
                    stream.Dispose();
                    return;
                }

                output(string.Format("Begin reading indexes"));

                if (jsonReader.TokenType != JsonToken.PropertyName)
                    throw new InvalidOperationException("PropertyName was expected");

                if (Equals("Indexes", jsonReader.Value) == false)
                    throw new InvalidOperationException("Indexes property was expected");

                if (jsonReader.Read() == false)
                    return;

                if (jsonReader.TokenType != JsonToken.StartArray)
                    throw new InvalidOperationException("StartArray was expected");

                // import Indexes
                WriteIndexes(jsonReader)
                    .ContinueOnSuccess(() =>
                    {
                        output(string.Format("Imported {0:#,#} indexes", totalIndexes));

                        output(string.Format("Begin reading documents"));

                        // should read documents now
                        if (jsonReader.Read() == false)
                        {
                            output("There were no documents to load");
                            stream.Dispose();
                            return;
                        }

                        if (jsonReader.TokenType != JsonToken.PropertyName)
                            throw new InvalidOperationException("PropertyName was expected");

                        if (Equals("Docs", jsonReader.Value) == false)
                            throw new InvalidOperationException("Docs property was expected");

                        if (jsonReader.Read() == false)
                        {
                            output("There were no documents to load");
                            stream.Dispose();
                            return;
                        }

                        if (jsonReader.TokenType != JsonToken.StartArray)
                            throw new InvalidOperationException("StartArray was expected");

                        WriteDocuments(jsonReader)
                            .ContinueOnSuccess(
                                () => output(string.Format("Imported {0:#,#} documents in {1:#,#} ms", totalCount,
                                                           sw.ElapsedMilliseconds)));
                    });
            }

            private Task WriteIndexes(JsonTextReader jsonReader)
            {

                while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
                {
                    var json = JToken.ReadFrom(jsonReader);
                    var indexName = json.Value<string>("name");
                    if (indexName.StartsWith("Temp/"))
                    {
                        continue;
                    }

                    var index = JsonConvert.DeserializeObject<IndexDefinition>(json.Value<JObject>("definition").ToString());

                    totalIndexes++;

                    output(string.Format("Importing index: {0}", indexName));

                    return asyncDatabaseCommands.PutIndexAsync(indexName, index, overwrite: true)
                        .ContinueOnSuccess(() => WriteIndexes(jsonReader));
                }

                var tcs = new TaskCompletionSource<object>();
                tcs.SetResult(null);
                return tcs.Task;
            }

            private Task WriteDocuments(JsonTextReader jsonReader)
            {
                var batch = new List<RavenJObject>();
                while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
                {
                    var document = RavenJToken.ReadFrom(jsonReader);
                    batch.Add((RavenJObject) document);
                    if (batch.Count >= BatchSize)
                    {
                        return FlushBatch(batch)
                            .ContinueOnSuccess(() => WriteDocuments(jsonReader));
                    }
                }
                return FlushBatch(batch);
            }

            Task FlushBatch(List<RavenJObject> batch)
            {
                totalCount += batch.Count;
                var sw = Stopwatch.StartNew();
                var commands = (from doc in batch
                                let metadata = doc.Value<RavenJObject>("@metadata")
                                let removal = doc.Remove("@metadata")
                                select new PutCommandData
                                {
                                    Metadata = metadata,
                                    Document = doc,
                                    Key = metadata.Value<string>("@id"),
                                }).ToArray();

                
                output(string.Format("Wrote {0} documents  in {1:#,#} ms",
                            batch.Count, sw.ElapsedMilliseconds));

                return asyncDatabaseCommands
                    .BatchAsync(commands);
            }

            private bool TryGetJsonReader(FileStream stream, out JsonTextReader jsonReader)
            {
                // Try to read the stream compressed, otherwise continue uncompressed.
                try
                {
                    var streamReader = new StreamReader(new GZipStream(stream, CompressionMode.Decompress));

                    jsonReader = new JsonTextReader(streamReader);

                    if (jsonReader.Read() == false)
                    {
                        output("Invalid json file found!");
                        return false;
                    }
                }
                catch (Exception)
                {
                    output(string.Format("Import file did not use GZip compression, attempting to read as uncompressed."));

                    stream.Seek(0, SeekOrigin.Begin);

                    var streamReader = new StreamReader(stream);

                    jsonReader = new JsonTextReader(streamReader);

                    if (jsonReader.Read() == false)
                    {
                        output("Invalid json file found!");
                        return false;
                    }
                }
                return true;
            }
        }

        public override ICommand Action
        {
            get { return new ImportDatabaseCommand(asyncDatabaseCommands, line => Output.Execute(() => Output.Add(line))); }
        }
    }
}
