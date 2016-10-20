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
using StressTests;
using Voron;

namespace Tryouts
{
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

            if (args.Length > 0 && args[0].Equals("--lz4test"))
            {
                LZ4Tester.DoTest();
                return;
            }

            if (args.Length > 0 && args[0].Equals("--singleTx"))
            {
                using (var massiveObj = new MassiveTest("http://localhost:8080", 1805861237))
                {
                    massiveObj.SendSingleTx();
                }
                return;
            }

            if (args.Length > 0 && args[0].Equals("--createdb"))
            {
                using (var store = new DocumentStore
                {
                    Url = "http://127.0.0.1:8080",
                    DefaultDatabase = args[1]
                })
                {
                    store.Initialize();
                    Util.CreateDb(store, args[1]);
                }
                return;
            }

            if (args.Length > 0)
            {
                MassiveTest.DoTest(Convert.ToInt32(args[0]));
                return;
            }

            Console.WriteLine();
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Usage : Tryouts < --import-pure-mem [inmem] | --import | bulkInsertNumFactor | --createdb <dbname> | --singleTx | --lz4test >   [--host host]");
            Console.ResetColor();
            Console.WriteLine();
        }
    }

    public class Util
    {
        public const long GB = 1024L * 1024 * 1024;
        public const long MB = 1024L * 1024;

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

        public static string PrintSize(long size)
        {
            float sum;
            string postfix;
            if (size >= GB)
            {
                sum = (size / GB);
                postfix = "GB";
            }
            else if (size >= MB)
            {
                sum = (size / MB);
                postfix = "MB";
            }
            else
            {
                sum = size;
                postfix = "B";
            }

            return $"{sum:#.##}{postfix} ({size:#,#})";
        }
    }

    public class Importer : RavenTestBase
    {
        public Task ImportTask(bool inMem)
        {
            using (var store = GetDocumentStore())
            {
                {
                    store.Initialize();
                    ImportData(store, inMem).Wait();
                }
                Console.WriteLine("End test");
                Console.ReadKey();
            }
            return Task.CompletedTask;
        }

        public static async Task ImportData(DocumentStore store, bool inMem)
        {
            var buf = new List<DocInfo>();
            long totalLen = 0;

            string filePath;
            if (Util.RunningOnPosix == false)
                filePath = @"C:\freedb.raven.dump";
            else
                filePath = @"/home/adi/Sources/freedb.raven.dump";

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

        public static void DoTest(int k = 1000)
        {
            using (var massiveObj = new MassiveTest("http://localhost:8080", 1805861237))
            {
                massiveObj.CreateDb();

                var kb = 1024;

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
                        Console.WriteLine(
                            $"{SystemTime.UtcNow.ToString("G")} : Progress {i:##,###} out of {numberOfDocuments} ...");

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

            Log($"Finished Bulk Insert {numberOfDocuments:#,#} documents of size {docSize:#,#} at " +
                elapsed.ToString("#,#") + " mSec");

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

        public void SendSingleTx()
        {
            var rc = _store.DatabaseCommands.Put("stamSingle/", null, new RavenJObject(), null);
            Console.WriteLine("key return = " + rc.Key ?? "null");
        }
    }

    public class LZ4Tester
    {
        const string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890 ";
        public static Random Rand = new Random(123);
        private static long BUFF_SIZE = 10 * Util.MB;

        public static void DoTest()
        {
            for (int i = 0; i < 10; i++)
            {
                Console.WriteLine("Run #" + i);
                var test = new HugeTransactions();
                test.LZ4TestAbove2GB(3L * 1024 * 1024);
            }

            for (int i = 0; i < 5; i++)
            {
                Console.WriteLine("Run BT 2 #" + i);
                var test = new HugeTransactions();
                test.CanWriteBigTransactions(2);
            }

            for (int i = 0; i < 3; i++)
            {
                Console.WriteLine("Run BT 6 #" + i);
                var test = new HugeTransactions();
                test.CanWriteBigTransactions(6);
            }


            Console.WriteLine("Press any key for next test");
            Console.ReadKey();


            BUFF_SIZE = 900 * Util.MB;

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(@"C:\zzzzTest")))
            {
                var value = new byte[BUFF_SIZE];
                Console.WriteLine("Filling " + Util.PrintSize(BUFF_SIZE) + " buffer with random values");
                new Random().NextBytes(value);

                Console.WriteLine("Add buffer");
                using (var tx = env.WriteTransaction())
                {
                    // env.Options.DataPager.EnsureContinuous(0, 256 * 1024);
                    var tree = tx.CreateTree("test1");

                    for (int i = 0; i < 8; i++)
                    {
                        var ms1 = new MemoryStream(value);
                        ms1.Position = 0;
                        tree.Add("treeKeyAA" + i, ms1);
                    }


                    tx.Commit();
                }

                Console.WriteLine("Add buffer 2");
                using (var tx = env.WriteTransaction())
                {
                    var tree = tx.CreateTree("test2");
                    var ms1 = new MemoryStream(value);
                    ms1.Position = 0;
                    tree.Add("treeKey12", ms1);

                    var ms2 = new MemoryStream(value);
                    ms2.Position = 0;
                    tree.Add("treeKey13", ms2);

                    tx.Commit();
                }
            }

            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Done !");
            Console.ResetColor();
        }
    }
}

