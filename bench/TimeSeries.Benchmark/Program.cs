using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
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
        private readonly int _workers;
        private const string DatabaseName = "TimeSeriesBenchmark";

        private readonly DocumentStore _store;
        private readonly string _workingDir;
        private CountdownEvent _fire;
        private TaskCompletionSource<bool> _onFire;

        private readonly Dictionary<string, string[]> _names = new Dictionary<string, string[]>();
        private int _measuresPerTimeSeries;
        private int _numberOfDocs;
        private int _timeSeriesPerDocument;
        private int _valuesPerMeasure;
        private int _interval;
        private DateTime _from;
        private DateTime _to;

        private int TotalTimeSeries => _timeSeriesPerDocument * _numberOfDocs;
        private int TotalMeasuresPerDocument => _timeSeriesPerDocument * _measuresPerTimeSeries * _valuesPerMeasure;
        private long TotalMetrics => TotalMeasuresPerDocument * _numberOfDocs;

        public TimeSeriesBench(string url, int workers, string workingDir = null)
        {
            _workers = workers;
            _store = new DocumentStore
            {
                Urls = new[] { url },
                Database = DatabaseName,
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            };
            _workingDir = workingDir ?? "./TimeSeriesBenchmarkData";
            _store.Initialize();
        }

        public class BenchParameters
        {
            public int Documents;
            public int TimeSeriesPerDocument;
            public int ValuesPerTimeSeries;
            public DateTime From;
            public DateTime To;
            public int IntervalMs;
        }

        public void Initialize(BenchParameters parameters)
        {
            Initialize(parameters.Documents, parameters.TimeSeriesPerDocument, parameters.ValuesPerTimeSeries, parameters.From, parameters.To, parameters.IntervalMs);
        }

        public void Initialize(int docs, int timeSeriesPerDocument, int valuesPerMeasure, DateTime from, DateTime to, int interval)
        {
            _numberOfDocs = docs;
            _timeSeriesPerDocument = timeSeriesPerDocument;
            _valuesPerMeasure = valuesPerMeasure;
            _from = from;
            _to = to;
            _interval = interval;
            _measuresPerTimeSeries = (int)((to - from).TotalMilliseconds / interval);

            _names.Clear();
            Log(
                $"Initialize to run with {_numberOfDocs:N0} documents, {_timeSeriesPerDocument:N0} time-series for each, {_valuesPerMeasure} metrics per measure. Total metrics {TotalMetrics:N0}.");
        }

        private DataGenerator GetDataGenerator()
        {
            return new DataGenerator(numberOfValues: _valuesPerMeasure, start: _from, millisecondsInterval: _interval);
        }

        public void Arm()
        {
            _onFire = new TaskCompletionSource<bool>();
            _fire = new CountdownEvent(Math.Min(TotalTimeSeries, _workers));
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

            private long _elapsedMilliseconds;

            private readonly int _numberOfValues;
            private readonly int _millisecondsInterval;
            private readonly bool _withTag;
            private readonly DateTime _baseline;
            private int _timeJitter = 20; // default is a jitter of 20%
            public DataGenerator(int numberOfValues = 1, DateTime? start = null, int millisecondsInterval = 10_000)
            {
                _numberOfValues = numberOfValues;
                _millisecondsInterval = millisecondsInterval;
                _withTag = false;
                _baseline = start ?? DateTime.UtcNow;
            }

            public void SetJitter(int jitter)
            {
                _timeJitter = jitter;
            }

            private double Jitter
            {
                get
                {
                    if (_timeJitter <= 0 || _timeJitter >= 100)
                        return 1;

                    var window = _timeJitter * 10;
                    return ((double)_rand.Next(1000 - window, 1000 + window) / 1000); 
                }
            }

            public void Reset()
            {
                _elapsedMilliseconds = 0;
            }

            public Measure Generate()
            {
                var interval = (int)(_millisecondsInterval * Jitter); // add some jitter
                _elapsedMilliseconds += interval;

                var m = new Measure
                {
                    TimeStamp = _baseline.AddMilliseconds(_elapsedMilliseconds), 
                    Values = new double[_numberOfValues], 
                };

                for (int i = 0; i < _numberOfValues; i++)
                {
                    m.Values[i] = (double)_rand.Next(-100_000, 100_000) / 1_000;
                }

                if (_withTag)
                {
                    m.Tag = GenerateTag();
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

            private readonly string[] _tagKeys = {"driver", "fleet", "model", "version"};

            private string GenerateTag()
            {
                var sb = new StringBuilder();
                foreach (var key in _tagKeys)
                {
                    sb.Append($"{key}={RandomString(6)} ");
                }

                return sb.ToString(0, sb.Length - 1);
            }

        }

        private async Task Worker(int workerIndex)
        {
            string tag = null;
            var path = Path.Combine(_workingDir, $"worker_{workerIndex}.ts-data");

            await using (var fileStream = new FileStream(path, FileMode.Open))
            await using (var gzip = new GZipStream(fileStream, CompressionMode.Decompress))
            using (var file = new StreamReader(gzip))
            await using (var bulk = _store.BulkInsert())
            {
                if (_onFire.Task.IsCompleted == false)
                {
                    _fire.Signal();
                    await _onFire.Task;
                }

                while (file.EndOfStream == false)
                {
                    var line = await file.ReadLineAsync();
                    if (line == null)
                        continue;

                    var parts = line.Split(',');

                    var document = parts[0];
                    var timeSeriesName = parts[1];
                    var count = long.Parse(parts[2]);

                    using (var ts = bulk.TimeSeriesFor(document, timeSeriesName))
                    {
                        for (int j = 0; j < count; j++)
                        {
                            line = await file.ReadLineAsync();
                            if (line == null)
                                continue;

                            parts = line.Split(',');
                            var time = new DateTime(long.Parse(parts[0]));
                            var numberOfValues = int.Parse(parts[1]);

                            var values = new double[numberOfValues];
                            for (int i = 2; i < numberOfValues + 2; i++)
                            {
                                values[i - 2] = double.Parse(parts[i]);
                            }
                            if (parts.Length > 2 + numberOfValues)
                                tag = parts[2 + numberOfValues];
                            await ts.AppendAsync(time, values, tag);
                        }
                    }
                }
            }
        }

        private class User
        {
            public string Id { get; set; }
        }

        private class DataPreparationParams
        {
            public readonly Dictionary<string, List<string>> Names = new Dictionary<string, List<string>>();
        }

        private DataPreparationParams[] _dataPreparationParams;

        private async Task CreateDocuments()
        {
            await using (var bulk = _store.BulkInsert())
            {
                foreach (var doc in _names.Keys)
                {
                    await bulk.StoreAsync(new User(), doc);
                }
            }
            Log($"Created {_numberOfDocs} documents");
        }

        private void InitializeLocalVariables()
        {
            _dataPreparationParams = new DataPreparationParams[_workers];
            var timeSeriesPerWorkerArray = new int[_workers];
            var timeSeriesPerWorker = TotalTimeSeries / _workers;
            var remaining = TotalTimeSeries % _workers;

            for (var index = 0; index < timeSeriesPerWorkerArray.Length; index++)
            {
                _dataPreparationParams[index] = new DataPreparationParams();
                timeSeriesPerWorkerArray[index] = timeSeriesPerWorker;
                if (remaining > 0)
                {
                    remaining--;
                    timeSeriesPerWorkerArray[index]++;
                }
            }

            var currentWorkerIndex = 0;
            var worker = _dataPreparationParams[currentWorkerIndex];
            var toTake = timeSeriesPerWorkerArray[currentWorkerIndex];

            _names.Clear();
            for (int i = 0; i < _numberOfDocs; i++)
            {
                var id = "users/" + (i + 1);
                _names[id] = new string[_timeSeriesPerDocument];
                for (int j = 0; j < _timeSeriesPerDocument; j++)
                {
                    var ts = "time-series-" + j;
                    _names[id][j] = "time-series-" + j;

                    if (toTake == 0)
                    {
                        currentWorkerIndex++;
                        worker = _dataPreparationParams[currentWorkerIndex];
                        toTake = timeSeriesPerWorkerArray[currentWorkerIndex];
                    }

                    if (toTake > 0)
                    {
                        toTake--;
                        if (worker.Names.ContainsKey(id) == false)
                            worker.Names[id] = new List<string>();
                        worker.Names[id].Add(ts);
                    }
                }
            }
            Log("Local variables are initialized.");
        }

        private async Task<string> DoInsert()
        {
            Arm();

            var sp = Stopwatch.StartNew();
            sp.Stop();

            var fired = Fire().ContinueWith(_ => sp.Restart());

            var tasks = new List<Task>();
            for (int i = 0; i < _workers; i++)
            {
                var t = Worker(i);
                tasks.Add(t);
            }
           
            await fired;
            await Task.WhenAll(tasks);

            sp.Stop();
            var rate =  _numberOfDocs * (TotalMeasuresPerDocument / sp.Elapsed.TotalSeconds);
            var stats = await _store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
            return $"Inserting is completed after {sp.Elapsed} with total rate of {(int)rate:N0} measures per second with {stats.SizeOnDisk.HumaneSize} size.";
        }

        public async Task BenchmarkInsert(int iteration)
        {
            await PrepareData();
            for (int i = 1; i <= iteration; i++)
            {
                Log("Start inserting data...");
                var result = await DoInsert();
                Log(result);
            }
        }

        public async Task BenchmarkQuery(int iteration)
        {
            Log("Start benching query");
            await SingleGroupBy(1, 1, 1);
            await SingleGroupBy(1, 1, 12);
            await SingleGroupBy(1, 8, 1);

            if (_valuesPerMeasure >= 5)
            {
                await SingleGroupBy(5, 1, 1);
                await SingleGroupBy(5, 1, 12);
                await SingleGroupBy(5, 8, 1);
            }
        }

        private async Task SingleGroupBy(int metrics, int series, int hours)
        {
            using (var session = _store.OpenAsyncSession())
            {
                var ids = _names.Keys.Take(series);
                var name = _names.First().Value[0];
                var to = _from.AddDays(3);
                var from = to.AddHours(-hours);

                var query = session.Query<User>()
                    .Where(u=>u.Id.In(ids))
                    .Statistics(out var stats)
                    .Select(u => RavenQuery.TimeSeries(u, name, from, to)
                        .GroupBy(g => g.Minutes(5))
                        .Select(g => new
                        {
                            Max = g.Max()
                        })
                        .ToList());
                var result = await query.ToListAsync();
                var total = 0;
                foreach (var aggResult in result)
                {
                    total += aggResult.Results.Length;
                }
                Log($"Done group by for {metrics} metrics, {series} hosts every 5 minutes for {hours} hours in {stats.DurationInMs} ms (total results: {total})");
            }
        }

        private async Task PrepareData()
        {
            var sp = Stopwatch.StartNew();
            Log($"Start preparing data for {_workers} workers.");

            InitializeLocalVariables();

            await CreateDocuments();

            for (int i = 0; i < _workers; i++)
            {
                await PrepareDataForWorker(i);
                Log($"Data for worker {i} is ready.");
            }

            Log($"Preparation of data is completed after {sp.Elapsed}.");
        }

        private async Task PrepareDataForWorker(int workerIndex)
        {
            var data = _dataPreparationParams[workerIndex];
            try
            {
                var path = Path.Combine(_workingDir, $"worker_{workerIndex}.ts-data");
                var sb = new StringBuilder();
                var generator = GetDataGenerator();

                await using (var fileStream = new FileStream(path, FileMode.Create))
                await using (var gzip = new GZipStream(fileStream, CompressionMode.Compress))
                await using (var fs = new StreamWriter(gzip))
                {
                    foreach (var names in data.Names)
                    foreach (var timeSeries in names.Value)
                    {
                        generator.Reset();
                        sb.AppendLine($"{names.Key},{timeSeries},{_measuresPerTimeSeries}");
                        for (int i = 0; i < _measuresPerTimeSeries; i++)
                        {
                            var measure = generator.Generate();
                            measure.AppendToBuilder(sb);
                            await fs.WriteLineAsync(sb);
                            sb.Clear();
                        } 
                    }
                }
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
            Directory.CreateDirectory(_workingDir);
            try
            {
                var doc = new DatabaseRecord(dbName);
                _store.Maintenance.Server.Send(new CreateDatabaseOperation(doc));
                _store.BeforeDispose += (o,e) =>
                {
                    try
                    {
             //           _store.Maintenance.Server.Send(new DeleteDatabasesOperation(dbName, hardDelete: true));
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
                var url = args[0];
                var workers = int.Parse(args[1]);

                var now = DateTime.UtcNow;

                var test1 = new BenchParameters
                {
                    Documents = 100,
                    TimeSeriesPerDocument = 1,
                    ValuesPerTimeSeries = 1,
                    From = now.AddMonths(-1),
                    To = now,
                    IntervalMs = 10_000
                };

                var test2 = new BenchParameters
                {
                    Documents = 100,
                    TimeSeriesPerDocument = 1,
                    ValuesPerTimeSeries = 10,
                    From = now.AddMonths(-1),
                    To = now,
                    IntervalMs = 10_000
                };

                var test3 = new BenchParameters
                {
                    Documents = 4000,
                    TimeSeriesPerDocument = 1,
                    ValuesPerTimeSeries = 10,
                    From = now.AddDays(-3),
                    To = now,
                    IntervalMs = 10_000
                };

                var test4 = new BenchParameters
                {
                    Documents = 100_000,
                    TimeSeriesPerDocument = 1,
                    ValuesPerTimeSeries = 10,
                    From = now.AddHours(-3),
                    To = now,
                    IntervalMs = 10_000
                };

                var test5 = new BenchParameters
                {
                    Documents = 1_000_000,
                    TimeSeriesPerDocument = 1,
                    ValuesPerTimeSeries = 10,
                    From = now.AddMinutes(-3),
                    To = now,
                    IntervalMs = 10_000
                };

                var simpleTest = new BenchParameters
                {
                    Documents = 4,
                    TimeSeriesPerDocument = 2,
                    ValuesPerTimeSeries = 1,
                    From = now.AddMonths(-1),
                    To = now,
                    IntervalMs = 1_000
                };

                using (var tsb = new TimeSeriesBench(url, workers))
                {
                    tsb.CreateDb(DatabaseName);
                    tsb.Initialize(test1);

                    await tsb.BenchmarkInsert(1);
                    await Task.Delay(10_000); // cooldown
                    Console.WriteLine();

                    await tsb.BenchmarkQuery(1);
                }
            }
        }
    }
}
