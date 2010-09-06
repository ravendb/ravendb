using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Media;
using System.Net;
using System.Text;
using System.Threading;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Server;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Bugs
{
    public class HiLoServerKeysNotExported : IDisposable
    {
        private DocumentStore documentStore;
        private RavenDbServer server;

        public HiLoServerKeysNotExported()
        {
            CreateServer(true);


        }

        private void CreateServer(bool initDocStore = false)
        {
            if (Directory.Exists("HiLoData")) Directory.Delete("HiLoData", true);
            server = new RavenDbServer(new RavenConfiguration { Port = 12345, DataDirectory = "HiLoData", RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true });

            if (initDocStore) {
                documentStore = new DocumentStore() { Url = "http://localhost:12345/" };
                documentStore.Initialize();
            }

            documentStore.DatabaseCommands.PutIndex("Foo/Something", new IndexDefinition<Foo> {
                Map = docs => from doc in docs select new { doc.Something }
            });

        }

        [Fact]
        public void Export_And_Import_Retains_HiLoState()
        {
            string firstId;
            using (var session = documentStore.OpenSession()) {
                var foo = new Foo() { Id = "foos/1", Something = "something" };
                session.Store(foo);
                Assert.Equal("foos/1", foo.Id);
                firstId = foo.Id;
                session.SaveChanges();
            }

            string secondId;
            using (var session = documentStore.OpenSession()) {
                var foo = new Foo() { Something = "something2" };
                Assert.Null(foo.Id);
                session.Store(foo);
                Assert.NotNull(foo.Id);
                Console.WriteLine("Second id = " + foo.Id);
                Assert.NotEqual(firstId, foo.Id);
                secondId = foo.Id;
                session.SaveChanges();
            }

            Console.WriteLine("Added 2 instances of foo with  id " + firstId + ", " + secondId);

            using (var session = documentStore.OpenSession()) {
                Assert.Equal(2, session.LuceneQuery<Foo>("Foo/Something")
                    .WaitForNonStaleResults()
                    .Count());
            }

            if (File.Exists("hilo-export.dump")) File.Delete("hilo-export.dump");
            ExportData("http://localhost:12345/", "hilo-export.dump", false);

            using (var session = documentStore.OpenSession()) {
                var hilo = session.Load<HiLoKey>("Raven/Hilo/foos");
                Assert.NotNull(hilo);
                Assert.Equal(2, hilo.ServerHi);
            }

            SystemSounds.Beep.Play();

            Thread.Sleep(10000);
            Assert.True(File.Exists("hilo-export.dump"));

            server.Dispose();
            CreateServer();

            Thread.Sleep(5000);
            using (var session = documentStore.OpenSession()) {
                Assert.Equal(0, session.LuceneQuery<Foo>("Foo/Something")
                    .WaitForNonStaleResults()
                    .Count());
            }

            ImportData("http://localhost:12345/", "hilo-export.dump");

            using (var session = documentStore.OpenSession()) {
                var hilo = session.Load<HiLoKey>("Raven/Hilo/foos");
                Assert.Null(hilo);
            }

            Thread.Sleep(5000);
            using (var session = documentStore.OpenSession()) {
                Assert.Equal(2, session.LuceneQuery<Foo>("Foo/Something")
                    .WaitForNonStaleResults()
                    .Count());
            }

            string thirdId;
            using (var session = documentStore.OpenSession()) {
                var foo = new Foo() { Something = "something3" };
                Assert.Null(foo.Id);
                session.Store(foo);
                Assert.NotNull(foo.Id);
                thirdId = foo.Id;
                session.SaveChanges();
            }

            Console.WriteLine("Added new instance of foo with  id " + thirdId);

            using (var session = documentStore.OpenSession()) {
                var hilo = session.Load<HiLoKey>("Raven/Hilo/foos");
                Assert.NotNull(hilo);
                Assert.Equal(3, hilo.ServerHi);
            }


            Assert.NotEqual(thirdId, firstId);
            Assert.NotEqual(thirdId, secondId);
        }

        public class Foo
        {
            public string Id { get; set; }
            public string Something { get; set; }
        }

        private class HiLoKey
        {
            public long ServerHi { get; set; }

        }

        public void Dispose()
        {
            documentStore.Dispose();
            server.Dispose();
            if (Directory.Exists("HiLoData")) Directory.Delete("HiLoData", true);
        }

        #region Copied from RavenSmuggler
        private static void ExportData(string instanceUrl, string file, bool exportIndexesOnly)
        {
            using (var streamWriter = new StreamWriter(new GZipStream(File.Create(file), CompressionMode.Compress))) {
                var jsonWriter = new JsonTextWriter(streamWriter) {
                    Formatting = Formatting.Indented
                };
                jsonWriter.WriteStartObject();
                jsonWriter.WritePropertyName("Indexes");
                jsonWriter.WriteStartArray();
                using (var webClient = new WebClient()) {
                    int totalCount = 0;
                    while (true) {
                        var documents = GetString(webClient.DownloadData(instanceUrl + "indexes?pageSize=128&start=" + totalCount));
                        var array = JArray.Parse(documents);
                        if (array.Count == 0) {
                            Console.WriteLine("Done with reading indexes, total: {0}", totalCount);
                            break;
                        }
                        totalCount += array.Count;
                        Console.WriteLine("Reading batch of {0,3} indexes, read so far: {1,10:#,#}", array.Count,
                                          totalCount);
                        foreach (JToken item in array) {
                            item.WriteTo(jsonWriter);
                        }
                    }
                }
                jsonWriter.WriteEndArray();
                jsonWriter.WritePropertyName("Docs");
                jsonWriter.WriteStartArray();

                if (!exportIndexesOnly) {
                    using (var webClient = new WebClient()) {
                        var lastEtag = Guid.Empty;
                        int totalCount = 0;
                        while (true) {
                            var documents =
                                GetString(webClient.DownloadData(instanceUrl + "docs?pageSize=128&etag=" + lastEtag));
                            var array = JArray.Parse(documents);
                            if (array.Count == 0) {
                                Console.WriteLine("Done with reading documents, total: {0}", totalCount);
                                break;
                            }
                            totalCount += array.Count;
                            Console.WriteLine("Reading batch of {0,3} documents, read so far: {1,10:#,#}", array.Count,
                                              totalCount);
                            foreach (JToken item in array) {
                                item.WriteTo(jsonWriter);
                            }
                            lastEtag = new Guid(array.Last.Value<JObject>("@metadata").Value<string>("@etag"));
                        }
                    }
                }
                jsonWriter.WriteEndArray();
                jsonWriter.WriteEndObject();
                streamWriter.Flush();
            }
        }

        public static string GetString(byte[] downloadData)
        {
            var ms = new MemoryStream(downloadData);
            return new StreamReader(ms, Encoding.UTF8).ReadToEnd();
        }

        public static void ImportData(string instanceUrl, string file)
        {
            var sw = Stopwatch.StartNew();

            using (FileStream fileStream = File.OpenRead(file)) {
                // Try to read the stream compressed, otherwise continue uncompressed.
                JsonTextReader jsonReader;

                try {
                    StreamReader streamReader = new StreamReader(new GZipStream(fileStream, CompressionMode.Decompress));

                    jsonReader = new JsonTextReader(streamReader);

                    if (jsonReader.Read() == false)
                        return;
                }
                catch (InvalidDataException) {
                    fileStream.Seek(0, SeekOrigin.Begin);

                    StreamReader streamReader = new StreamReader(fileStream);

                    jsonReader = new JsonTextReader(streamReader);

                    if (jsonReader.Read() == false)
                        return;
                }

                if (jsonReader.TokenType != JsonToken.StartObject)
                    throw new InvalidDataException("StartObject was expected");

                // should read indexes now
                if (jsonReader.Read() == false)
                    return;
                if (jsonReader.TokenType != JsonToken.PropertyName)
                    throw new InvalidDataException("PropertyName was expected");
                if (Equals("Indexes", jsonReader.Value) == false)
                    throw new InvalidDataException("Indexes property was expected");
                if (jsonReader.Read() == false)
                    return;
                if (jsonReader.TokenType != JsonToken.StartArray)
                    throw new InvalidDataException("StartArray was expected");
                using (var webClient = new WebClient()) {
                    webClient.UseDefaultCredentials = true;
                    webClient.Headers.Add("Content-Type", "application/json; charset=utf-8");
                    webClient.Credentials = CredentialCache.DefaultNetworkCredentials;
                    while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray) {
                        var index = JToken.ReadFrom(jsonReader);
                        var indexName = index.Value<string>("name");
                        if (indexName.StartsWith("Raven/"))
                            continue;
                        using (var streamWriter = new StreamWriter(webClient.OpenWrite(instanceUrl + "indexes/" + indexName, "PUT")))
                        using (var jsonTextWriter = new JsonTextWriter(streamWriter)) {
                            index.Value<JObject>("definition").WriteTo(jsonTextWriter);
                            jsonTextWriter.Flush();
                            streamWriter.Flush();
                        }
                    }
                }
                // should read documents now
                if (jsonReader.Read() == false)
                    return;
                if (jsonReader.TokenType != JsonToken.PropertyName)
                    throw new InvalidDataException("PropertyName was expected");
                if (Equals("Docs", jsonReader.Value) == false)
                    throw new InvalidDataException("Docs property was expected");
                if (jsonReader.Read() == false)
                    return;
                if (jsonReader.TokenType != JsonToken.StartArray)
                    throw new InvalidDataException("StartArray was expected");
                var batch = new List<JObject>();
                int totalCount = 0;
                while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray) {
                    totalCount += 1;
                    var document = JToken.ReadFrom(jsonReader);
                    batch.Add((JObject)document);
                    if (batch.Count >= 128)
                        FlushBatch(instanceUrl, batch);
                }
                FlushBatch(instanceUrl, batch);
                Console.WriteLine("Imported {0:#,#} documents in {1:#,#} ms", totalCount, sw.ElapsedMilliseconds);
            }
        }

        private static void FlushBatch(string instanceUrl, List<JObject> batch)
        {
            var sw = Stopwatch.StartNew();
            long size;
            using (var webClient = new WebClient()) {
                webClient.Headers.Add("Content-Type", "application/json; charset=utf-8");
                webClient.UseDefaultCredentials = true;
                webClient.Credentials = CredentialCache.DefaultNetworkCredentials;
                using (var stream = new MemoryStream()) {
                    using (var streamWriter = new StreamWriter(stream, Encoding.UTF8))
                    using (var jsonTextWriter = new JsonTextWriter(streamWriter)) {
                        var commands = new JArray();
                        foreach (var doc in batch) {
                            var metadata = doc.Value<JObject>("@metadata");
                            doc.Remove("@metadata");
                            commands.Add(new JObject(
                                             new JProperty("Method", "PUT"),
                                             new JProperty("Document", doc),
                                             new JProperty("Metadata", metadata),
                                             new JProperty("Key", metadata.Value<string>("@id"))
                                             ));
                        }
                        commands.WriteTo(jsonTextWriter);
                        jsonTextWriter.Flush();
                        streamWriter.Flush();
                        stream.Flush();
                        size = stream.Length;

                        using (var netStream = webClient.OpenWrite(instanceUrl + "bulk_docs", "POST")) {
                            stream.WriteTo(netStream);
                            netStream.Flush();
                        }
                    }
                }

            }
            Console.WriteLine("Wrote {0} documents [{1:#,#} kb] in {2:#,#} ms",
                              batch.Count, Math.Round((double)size / 1024, 2), sw.ElapsedMilliseconds);
            batch.Clear();
        }
        #endregion
    }
}
