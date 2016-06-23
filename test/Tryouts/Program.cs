using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Json;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using FastTests;

namespace Tryouts
{
    public class Util
    {
        public static readonly bool RunningOnPosix = RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ||
                                                     RuntimeInformation.IsOSPlatform(OSPlatform.OSX);

        public static void CreateDb(DocumentStore store, string dbname = null)
        {
            try
            {
                if (dbname == null)
                    dbname = "test";
                var doc = MultiDatabase.CreateDatabaseDocument(dbname);
                store.AsyncDatabaseCommands.GlobalAdmin.CreateDatabaseAsync(doc).Wait();
            }
            catch (Exception ex)
            {
                if (ex.Message.Contains("already exists"))
                {
                    Console.WriteLine($"Database '{dbname}' already exists!");
                }
                else
                {
                    Console.WriteLine("Cannot create DB " + dbname + ". Exception : " + ex.Message, true);
                    throw;
                }
            }
        }
    }

    public class Importer : RavenTestBase
    {
        public async Task ImportTask(bool inMem)
        {
            using (var store = await GetDocumentStore().ConfigureAwait(false))
            {
                {
                    store.Initialize();
                    ImportData(store, inMem).Wait();
                }
                Console.WriteLine("End test");
                Console.ReadKey();
            }
        }

        public static async Task ImportData(DocumentStore store, bool inMem)
        {
            var buf = new List<DocInfo>();
            long totalLen = 0;

            var filePath = Util.RunningOnPosix == false ? @"C:\freedb.raven.dump" : @"/home/adi/Sources/freedb.raven.dump";

            Stream dumpStream = File.OpenRead(filePath);
            var gZipStream = new GZipStream(dumpStream, CompressionMode.Decompress, leaveOpen: true);
            using (var streamReader = new StreamReader(gZipStream))
            using (var reader = new RavenJsonTextReader(streamReader))
            {

                if (reader.Read() == false /* { */|| reader.Read() == false /* prop*/)
                    throw new InvalidOperationException("empty document?");

                if (reader.TokenType != JsonToken.PropertyName)
                    throw new InvalidOperationException("Expected property");

                if ((string)reader.Value != "Docs")
                    throw new InvalidOperationException("Expected property name 'Docs'");

                if (reader.Read() == false)
                    throw new InvalidOperationException("corrupt document");

                if (reader.TokenType != JsonToken.StartArray)
                    throw new InvalidOperationException("corrupt document, missing array");

                if (reader.Read() == false)
                    throw new InvalidOperationException("corrupt document, array value");



                var sp = new Stopwatch();

                if (inMem == false)
                {
                    sp.Start();
                    int i = 0;
                    using (var bulk = store.BulkInsert())
                    {
                        while (reader.TokenType != JsonToken.EndArray)
                        {
                            var document = RavenJObject.Load(reader);
                            var metadata = document.Value<RavenJObject>("@metadata");
                            var key = metadata.Value<string>("@id");
                            document.Remove("@metadata");
                            await bulk.StoreAsync(document, metadata, key).ConfigureAwait(false);

                            if (i % (100 * 1000) == 0)
                                Console.WriteLine($"Progress {i:N} ...");
                            i++;

                            if (reader.Read() == false)
                                throw new InvalidOperationException("corrupt document, array value");
                        }
                    }
                    sp.Stop();
                }
                else
                {
                    // in mem:
                    int i = 0;

                    while (reader.TokenType != JsonToken.EndArray)
                    {
                        var document = RavenJObject.Load(reader);
                        var metadata = document.Value<RavenJObject>("@metadata");
                        var key = metadata.Value<string>("@id");
                        document.Remove("@metadata");
                        // await bulk.StoreAsync(document, metadata, key).ConfigureAwait(false);

                        var inf = new DocInfo
                        {
                            Document = document,
                            MetaData = metadata,
                            Key = key
                        };

                        buf.Add(inf);

                        //  totalLen += document.ToString().Length + metadata.ToString().Length + key.Length;

                        if (i % (100 * 1000) == 0)
                            Console.WriteLine($"Progress {i:N} ...");
                        i++;
                        if (i == 1 * 1000 * 1000)
                            break;
                        if (reader.Read() == false)
                            throw new InvalidOperationException("corrupt document, array value");
                    }


                    using (var bulk = store.BulkInsert())
                    {
                        sp.Start();

                        foreach (var x in buf)
                        {
                            await bulk.StoreAsync(x.Document, x.MetaData, x.Key).ConfigureAwait(false);
                        }

                    }
                }
                sp.Stop();

                Console.WriteLine($"Ellapsed time = {sp.ElapsedMilliseconds:#,#}, total={totalLen:#,#}");
            }
        }
    }

    public class DocInfo
    {
        public RavenJObject Document;
        public RavenJObject MetaData;
        public string Key;
    }


    public class MassiveTest : IDisposable
    {
        private readonly Random _seedRandom = new Random();
        private readonly Random _random;
        private string _logFilename;
        private readonly DocumentStore _store;
        private long _seqId;

        private class DocEntity
        {
            public int RandomId { get; set; }
            public long SerialId { get; set; }
            public object SomeRandomText { get; set; }
            public string[] Tags { get; set; }
        }

        public MassiveTest(string url, int? seed = null)
        {
            int usedSeed;
            if (seed == null)
                usedSeed = _seedRandom.Next();
            else
                usedSeed = seed.Value;

            _random = new Random(usedSeed);

            Log("Seed = " + usedSeed);

            _store = new DocumentStore
            {
                Url = url,
                DefaultDatabase = "test"
            };

            _store.Initialize();
        }

        private void Log(string txt, bool isError = false)
        {
            if (_logFilename == null)
            {
                _logFilename = Path.GetTempFileName();
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("Loggin into " + _logFilename);
                Console.ResetColor();
            }
            var errorStr = isError ? " *** ERROR ***" : "";
            string txtToLog = SystemTime.UtcNow.ToString("G", new CultureInfo("he-il")) + errorStr + " : " + txt;
            File.AppendAllText(_logFilename, txtToLog + Environment.NewLine);

            Console.WriteLine(txtToLog);
        }

        public void CreateDb(string dbname = null)
        {
            try
            {
                if (dbname == null)
                    dbname = "test";
                var doc = MultiDatabase.CreateDatabaseDocument(dbname);
                _store.AsyncDatabaseCommands.GlobalAdmin.CreateDatabaseAsync(doc).Wait();
            }
            catch (Exception ex)
            {
                if (ex.Message.Contains("already exists"))
                {
                    Log($"Database '{dbname}' already exists!");
                }
                else
                {
                    Log("Cannot create DB " + dbname + ". Exception : " + ex.Message, true);
                    throw;
                }
            }
        }

        private string RandomString(int length)
        {
            // return new string(Enumerable.Repeat('A', length).ToArray());
            const string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890 ";
            return new string(Enumerable.Repeat(chars, length)
                .Select(s => s[_random.Next(s.Length)]).ToArray());
        }

        public void PerformBulkInsert(string collection, long numberOfDocuments, int? sizeOfDocuments,
            bool useSeqId = true)
        {
            var docSize = sizeOfDocuments == null ? _random.Next(5 * 1024 * 1024) : sizeOfDocuments.Value;
            var sizeStr = sizeOfDocuments == null ? $"Random Size of {docSize:#,#}" : $"size of {sizeOfDocuments:#,#}";
            Log($"About to perform Bulk Insert for {numberOfDocuments:##,###} documents with " + sizeStr);

            var entities = new DocEntity[3];
            var ids = new long[] { -1, -1, -1 };

            string[] tags = null;
            long id = 1;
            var sp = Stopwatch.StartNew();
            using (var bulkInsert = _store.BulkInsert())
            {
                for (long i = 0; i < numberOfDocuments; i++)
                {
                    if (i % (numberOfDocuments / 5) == 0)
                        Console.WriteLine($"{SystemTime.UtcNow.ToString("G")} : Progress {i:##,###} out of {numberOfDocuments} ...");

                    var entity = new DocEntity
                    {
                        SerialId = i,
                        RandomId = _random.Next(),
                        SomeRandomText = RandomString(docSize),
                        Tags = tags
                    };
                    var idToUse = useSeqId ? _seqId++ : id++;
                    bulkInsert.StoreAsync(entity, $"{collection}/{idToUse}").Wait();

                    if (i == 0)
                    {
                        ids[0] = idToUse;
                        entities[0] = entity;
                    }
                    else if (i == numberOfDocuments / 2)
                    {
                        ids[1] = idToUse;
                        entities[1] = entity;
                    }
                    else if (i == numberOfDocuments - 1)
                    {
                        ids[2] = idToUse;
                        entities[2] = entity;
                    }
                }

                if (ids[0] == -1 ||
                    ids[1] == -1 ||
                    ids[2] == -1)
                    throw new Exception("Internal Error");
            }

            var elapsed = sp.ElapsedMilliseconds;

            Log($"Finished Bulk Insert {numberOfDocuments:#,#} documents of size {docSize:#,#} at " + elapsed.ToString("#,#") + " mSec");

            if (sizeOfDocuments == null)
            {
                Log($"STATISTICS: N/A - Random Size");
            }
            else
            {
                long totalSize = numberOfDocuments * docSize;
                var totalRate = (totalSize / elapsed) / 1000;
                Log($"STATISTICS: Total Size {totalSize:#,#}, {totalRate:#,#} MB/Sec");
            }


            using (var session = _store.OpenSession())
            {
                var first = session.Load<DocEntity>($"{collection}/{ids[0]}");
                var middle = session.Load<DocEntity>($"{collection}/{ids[1]}");
                var last = session.Load<DocEntity>($"{collection}/{ids[2]}");

                if (first == null || middle == null || last == null)
                {
                    Log(
                        $"Data Verification Failed : isNull :: first={first == null}, middle={middle == null}, last={last == null}",
                        true);
                }
                else if (first.SerialId == 0 &&
                    middle.SerialId == numberOfDocuments / 2 &&
                    last.SerialId == numberOfDocuments - 1)
                {
                    Log("Data Successfully Verified!");
                }
                else
                {
                    Log(
                        $"Data Verification Failed : first={first.SerialId}, middle={middle.SerialId}, last={last.SerialId}",
                        true);
                }
            }

        }

        public void Dispose()
        {
            _store.Dispose();
        }
    }

    public class Program
    {
        public static void Main(string[] args)
        {
            if (args.Length > 0 && args[0].Equals("--import"))
            {
                // inMem - import file to memory first and then to database storage
                var inMem = args.Length > 1 && args[1].Equals("inmem", StringComparison.OrdinalIgnoreCase);
                using (var store = new DocumentStore
                {
                    Url = "http://127.0.0.1:8080",
                    DefaultDatabase = "test"
                })
                {
                    store.Initialize();
                    Util.CreateDb(store);
                    var importTask = Importer.ImportData(store, inMem);
                    importTask.Wait();
                }
                Console.WriteLine("End test");
                Console.ReadKey();
                return;
            }

            if (args.Length > 0 && args[0].Equals("--import-pure-mem"))
            {
                var inMem = args.Length > 1 && args[1].Equals("inmem", StringComparison.OrdinalIgnoreCase);
                var d = new Importer();
                d.ImportTask(inMem).Wait();
                Console.WriteLine("End test");
                Console.ReadKey();
                return;
            }

            using (var massiveObj = new MassiveTest("http://localhost:8080", 1805861237))
            {
                massiveObj.CreateDb();

                var k = 1000;
                var kb = 1024;

                if (args.Length == 1)
                    k = Convert.ToInt32(args[0]);

                //Console.WriteLine("metoraf...");
                //massiveObj.PerformBulkInsert("warmup", 1000 * k, 30 * kb * kb);

                Console.WriteLine("warmup...");
                massiveObj.PerformBulkInsert("warmup", 10 * k, 2 * kb);
                Console.WriteLine("smallSize...");
                massiveObj.PerformBulkInsert("smallSize", 2 * k * k, 2 * kb);
                Console.WriteLine("bigSize...");
                massiveObj.PerformBulkInsert("bigSize", 1 * k, 30 * kb * kb);
                Console.WriteLine("forOverrite...");
                massiveObj.PerformBulkInsert("forOverrite", 500 * k, 5 * kb, false);
                Console.WriteLine("forOverrite2...");
                massiveObj.PerformBulkInsert("forOverrite", 1000 * k, 5 * kb, false);
                for (int i = 1; i <= 5; i++)
                {
                    Console.WriteLine($"random {i}...");
                    massiveObj.PerformBulkInsert("random", i * k * k, null);
                }

                Console.WriteLine("Ending tests...");
            }
        }
    }
}