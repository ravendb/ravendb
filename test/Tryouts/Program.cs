using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using Raven.Abstractions;
using Raven.Client.Document;
using Raven.Client.Extensions;

namespace Tryouts
{
    public class MassiveTest : IDisposable
    {
        private Random _seedRandom = new Random();
        private int _seed;
        private Random _random;
        private string _logFilename;
        private DocumentStore store;
        private long _seqId;

        private class DocEntity
        {
            public object RandomId { get; set; }
            public long SerialId { get; set; }
            public object SomeRandomText { get; set; }
            public string[] Tags { get; set; }
        }

        public MassiveTest(string url, int? seed = null)
        {
            if (seed == null)
                _seed = _seedRandom.Next();
            else
                _seed = seed.Value;

            _random = new Random(_seed);

            Log("Seed = " + _seed);

            store = new DocumentStore
            {
                Url = url,
                DefaultDatabase = "test"
            };

            store.Initialize();
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
                store.AsyncDatabaseCommands.GlobalAdmin.CreateDatabaseAsync(doc).Wait();
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
            const string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890 ";
            return new string(Enumerable.Repeat(chars, length)
                .Select(s => s[_random.Next(s.Length)]).ToArray());
        }

        public void PerformBulkInsert(string collection, long numberOfDocuments, int? sizeOfDocuments,
            bool useSeqId = true)
        {
            int docSize = sizeOfDocuments == null ? _random.Next(5 * 1024 * 1024) : sizeOfDocuments.Value;
            var sizeStr = sizeOfDocuments == null ? $"Random Size of {docSize:#,#}" : $"size of {sizeOfDocuments:#,#}";
            Log($"About to perform Bulk Insert for {numberOfDocuments:##,###} documents with " + sizeStr);

            var entities = new DocEntity[3];
            var ids = new long[] { -1, -1, -1 };

            string[] tags = null;
            long id = 1;
            var sp = Stopwatch.StartNew();
            using (var bulkInsert = store.BulkInsert())
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


            using (var session = store.OpenSession())
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

                if (first.SerialId == 0 &&
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
            store.Dispose();
        }
    }

    public class Program
    {
        public static void Main(string[] args)
        {
            using (var massiveObj = new MassiveTest("http://127.0.0.1:8080", 1805861237))
            {
                massiveObj.CreateDb();

                var k = 1000;
                var kb = 1024;

                if (args.Length == 1)
                    k = Convert.ToInt32(args[0]);

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
