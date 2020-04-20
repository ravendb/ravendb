using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;

namespace TimeSeries.Benchmark
{
    public class TimeSeriesBench : IDisposable
    {
        private const string DatabaseName = "TimeSeriesBenchmark";
        private DataGenerator _dataGenerator;

        private readonly DocumentStore _store;
        private readonly string _workingDir;
        private CountdownEvent _fire;
        private TaskCompletionSource<bool> _onFire;

        private readonly Dictionary<string, string[]> _names = new Dictionary<string, string[]>();
        private int _measuresPerTimeSeries;
        private int _numberOfDocs;
        private int _timeSeriesPerDocument;

        private int TotalTimeSeries => _timeSeriesPerDocument * _numberOfDocs;
        private int TotalMeasuresPerDocument => _timeSeriesPerDocument * _measuresPerTimeSeries;
        
        public TimeSeriesBench(string url)
        {
            _store = new DocumentStore
            {
                Urls = new[] { url },
                Database = DatabaseName,
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            };
            _workingDir = "./TimeSeriesBenchmarkData";
            _store.Initialize();
        }

        public void Initialize(int docs, int timeSeriesPerDocument, int measuresPerTimeSeries, int valuesPerMeasure)
        {
            _numberOfDocs = docs;
            _timeSeriesPerDocument = timeSeriesPerDocument;
            _measuresPerTimeSeries = measuresPerTimeSeries;
            _dataGenerator = new DataGenerator(numberOfValues: valuesPerMeasure);
            _names.Clear();
            Log($"Initialize to run with {_numberOfDocs:N0} documents with {_timeSeriesPerDocument:N0} time-series for each and {_measuresPerTimeSeries:N0} measurements per single time-series of {valuesPerMeasure} dimensions.");
        }

        public void Arm()
        {
            _onFire = new TaskCompletionSource<bool>();
            _fire = new CountdownEvent(TotalTimeSeries);
        }

        public Task Fire()
        {
            Task.Run(()=>
            {
                _fire.Wait();
                _onFire.TrySetResult(true);
            });
            return _onFire.Task;
        }

        private class Measure
        {
            public DateTime TimeStamp;
            public double[] Values;
            public string Tag;

            public void AppendToBuilder(StringBuilder sb)
            {
                sb.Append($"{TimeStamp.Ticks},{Values.Length}");
                foreach (var v in Values)
                {
                    sb.Append($",{Convert.ToString(v, CultureInfo.InvariantCulture)}");
                }
                if (Tag != null)
                    sb.Append($",{Tag}");
            }
        }

        private class DataGenerator
        {
            private const string _chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890 ";

            [ThreadStatic]
            private static Random _currentThreadRand;
            private Random _rand => _currentThreadRand ??= new Random();

            private static long ElapsedMilliseconds;

            private readonly int _maxTagLength;
            private readonly int _numberOfValues;
            private readonly bool _withTag;
            private readonly DateTime _baseline;

            public DataGenerator(int maxTagLength = 0, int numberOfValues = 1, DateTime? start = null)
            {
                _maxTagLength = maxTagLength;
                _numberOfValues = numberOfValues;
                _withTag = _maxTagLength > 0;
                _baseline = start ?? DateTime.UtcNow;
            }
            public Measure Generate(int millisecondsInterval = 1000)
            {
                var interval = (int)(millisecondsInterval * ((double)_rand.Next(8, 12) / 10)); // add some jitter
                var elapsed = Interlocked.Add(ref ElapsedMilliseconds, interval);
                var m = new Measure
                {
                    TimeStamp = _baseline.AddMilliseconds(elapsed), 
                    Values = new double[_numberOfValues], 
                    Tag = _withTag == false ? null : RandomString(_maxTagLength)
                };

                for (int i = 0; i < _numberOfValues; i++)
                {
                    m.Values[i] = (double)_rand.Next(-100_000, 100_000) / 1_000;
                }

                return m;
            }

            private string RandomString(int length)
            {
                var str = new char[length];
                for (int i = 0; i < length; i++)
                {
                    str[i] = _chars[_rand.Next(_chars.Length)];
                }
                return new string(str);
            }
        }

        private async Task Worker(string documentId, string name)
        {
            var sp = Stopwatch.StartNew();
            sp.Stop();
     
            var measures = 0;
            string tag = null;

            await using (var bulk = _store.BulkInsert())
            using (var ts = bulk.TimeSeriesFor(documentId, name))
            using (var file = new StreamReader(Path.Combine(_workingDir, documentId, name + ".ts-data")))
            {
                _fire.Signal();
                await _onFire.Task;
                sp.Restart();

                while (file.EndOfStream == false)
                {
                    var line = await file.ReadLineAsync();
                    if (line == null)
                        continue;

                    var parts = line.Split(',');
                    var time = new DateTime(long.Parse(parts[0]));
                    var numberOfValues = int.Parse(parts[1]);

                    // we have only one value
                    if (numberOfValues == 1)
                    {
                        var value = double.Parse(parts[2]);
                        if (parts.Length > 3)
                            tag = parts[3];
                        measures++;
                        await ts.AppendAsync(time, value, tag);
                        continue;
                    }

                    // we have several values
                    var values = new double[numberOfValues];
                    for (int i = 2; i < numberOfValues + 2; i++)
                    {
                        values[i - 2] = double.Parse(parts[i]);
                    }
                    if (parts.Length > 2 + numberOfValues)
                        tag = parts[2 + numberOfValues];
                    measures++;
                    await ts.AppendAsync(time, values, tag);
                }
            }
            
            sp.Stop();
            var rate = measures / (sp.Elapsed.TotalSeconds);
            Log($"Finished inserting into document '{documentId}' time-series '{name}' with {measures:N0} measurements in {sp.Elapsed} ({(int)rate:N0} measures per second)");
        }

        private class User
        {
            public string Id { get; set; }
        }

        private async Task CreateDocuments()
        {
            _names.Clear();
            for (int i = 0; i < _numberOfDocs; i++)
            {
                var id = "users/" + (i + 1);
                _names[id] = new string[_timeSeriesPerDocument];
                for (int j = 0; j < _timeSeriesPerDocument; j++)
                {
                    _names[id][j] = "time-series-" + j;
                }
            }

            await using (var bulk = _store.BulkInsert())
            {
                foreach (var doc in _names.Keys)
                {
                    await bulk.StoreAsync(new User(), doc);
                }
            }
        }

        private async Task<string> DoInsert()
        {
            Arm();

            var sp = Stopwatch.StartNew();
            sp.Stop();

            var fired = Fire().ContinueWith(_ => sp.Restart());

            var tasks = new List<Task>();
            foreach (var kvp in _names)
            {
                foreach (var ts in kvp.Value)
                {
                    var t = Worker(kvp.Key, ts);
                    tasks.Add(t);
                }
            }
            await fired;
            await Task.WhenAll(tasks);

            sp.Stop();
            var rate = _numberOfDocs * (TotalMeasuresPerDocument / sp.Elapsed.TotalSeconds);
            var stats = await _store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
            return $"Test is completed after {sp.Elapsed} with total rate of {(int)rate:N0} points per second with {stats.SizeOnDisk.HumaneSize} size.";
        }

        public async Task BenchmarkInsert(int iteration)
        {
            await PrepareData();
            for (int i = 1; i <= iteration; i++)
            {
                Log("Start bulk insert iteration "+i);
                var result = await DoInsert();
                Log(result);
            }
        }

        public async Task BenchmarkQuery(int iteration)
        {
            Log("Start benching query");
            await Aggregation();
        }

        private async Task Aggregation()
        {
            using (var session = _store.OpenAsyncSession())
            {
                var id = _names.Keys.ToArray()[0];
                var name = _names[id][0];

                var query = session.Query<User>()
                    .Where(u => u.Id == id)
                    .Statistics(out var stats)
                    .Select(u => RavenQuery.TimeSeries(u, name)
                        .GroupBy(g => g.Minutes(5))
                        .Select(g => new
                        {
                            Max = g.Max()
                        })
                        .ToList());
                var result = await query.ToListAsync();
                Log($"Done aggregation by day in {stats.DurationInMs} ms with total days of {result[0].Results.Length}");
            }
        }

        private async Task PrepareData()
        {
            await CreateDocuments();

            var tasks = new List<Task>();
            foreach (var kvp in _names)
            {
                tasks.Clear();
                foreach (var ts in kvp.Value)
                {
                    var t = PrepareDataForSingleInsert(kvp.Key, ts, _measuresPerTimeSeries);
                    tasks.Add(t);
                }
                await Task.WhenAll(tasks);
            }

            Log("Data preparation is completed.");
        }

        private async Task PrepareDataForSingleInsert(string documentId, string name, int numberOfMeasures)
        {
            Log($"Start preparing {numberOfMeasures:N0} measures for document '{documentId}' and time-series '{name}'");

            try
            {
                var dir = Path.Combine(_workingDir, documentId);
                Directory.CreateDirectory(dir);
                var file = Path.Combine(dir, name + ".ts-data");

                await using (var fs = new StreamWriter(file))
                {
                    var sb = new StringBuilder();
                    for (int i = 0; i < numberOfMeasures; i++)
                    {
                        var measure = _dataGenerator.Generate();
                        measure.AppendToBuilder(sb);
                        await fs.WriteLineAsync(sb);
                        sb.Clear();
                    }
                }

                Log($"Finished preparation for document '{documentId}' and time-series '{name}'");
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        private void Log(string txt, bool isError = false)
        {
            var errorStr = isError ? " *** ERROR ***" : "";
            string txtToLog = SystemTime.UtcNow.ToString("G", new CultureInfo("he-il")) + errorStr + " : " + txt;
            Console.WriteLine(txtToLog);
        }

        public void CreateDb(string dbName)
        {
            try
            {
                var doc = new DatabaseRecord(dbName);
                _store.Maintenance.Server.Send(new CreateDatabaseOperation(doc));
                _store.BeforeDispose += (o,e) =>
                {
                    try
                    {
                        _store.Maintenance.Server.Send(new DeleteDatabasesOperation(dbName, hardDelete: true));
                    }
                    catch (Exception ex)
                    {
                        Log("Cannot delete DB " + dbName + ". Exception : " + ex.Message, true);
                    }
                };
            }
            catch (Exception ex)
            {
                if (ex.Message.Contains("already exists"))
                {
                    Log($"Database '{dbName}' already exists!");
                }
                else
                {
                    Log("Cannot create DB " + dbName + ". Exception : " + ex.Message, true);
                    throw;
                }
            }
        }

        public void Dispose()
        {
            _store.Dispose();
            Directory.Delete(_workingDir, recursive: true);
        }

        public class Program
        {
            public static async Task Main(string[] args)
            {
                var documents = 1;
                var timeSeriesPerDocument = 5;
                var measuresPerTimeSeries = 1_000_000;
                var valuesPerMeasure = 1;

                using (var tsb = new TimeSeriesBench("http://localhost:8080"))
                {
                    tsb.CreateDb(DatabaseName);
                    tsb.Initialize(documents, timeSeriesPerDocument, measuresPerTimeSeries, valuesPerMeasure);

                    await tsb.BenchmarkInsert(1);
                    await tsb.BenchmarkQuery(1);
                }
            }
        }
    }
}
