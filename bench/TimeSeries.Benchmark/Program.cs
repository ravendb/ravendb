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
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;

namespace TimeSeries.Benchmark
{
    public class TimeSeriesBench : IDisposable
    {
        public class Program
        {
            public static async Task Main(string[] args)
            {
                var benchParameters = ParseArguments(args, out string url, out int workers);

                using (var tsb = new TimeSeriesBench(url, workers))
                {
                    tsb.Initialize(benchParameters);

                    await tsb.BenchmarkInsert();
                    await Task.Delay(10_000); // cooldown
                    await tsb.BenchmarkQuery();
                    Console.WriteLine();
                }
            }

            private static BenchParameters ParseArguments(string[] args, out string url, out int workers)
            {
                url = null;
                workers = 0;

                var benchParameters = new BenchParameters
                {
                    IntervalMs = 10_000,
                    TimeSeriesPerDocument = 1,
                    ValuesPerTimeSeries = 1,
                    Cleanup = false
                };

                /* example usage
                --url=http://172.31.9.44 --workers=8 --scale=100 --metrics=1 --start=3/20 --end=4/20 				// month span
                --url=http://172.31.9.44 --workers=8 --scale=100 --metrics=10 --start=3/20 --end=4/20 				// month span
                --url=http://172.31.9.44 --workers=8 --scale=4000 --metrics=10 --start=08/18/2018 --end=08/21/2018 	// 3 days span
                --url=http://172.31.9.44 --workers=8 --scale=100000 --metrics=10 --start=07:22:16 --end=10:22:16 	// 3 hours span
                --url=http://172.31.9.44 --workers=8 --scale=1000000 --metrics=10 --start=07:22:1 --end=07:25:16 	// 3 minutes span
                 */
                foreach (var arg in args)
                {
                    var kvp = arg.Split('=');
                    var key = kvp[0].Trim();
                    var val = kvp[1].Trim();
                    switch (key)
                    {
                        case "--url":
                            url = val;
                            break;
                        case "--workers":
                            workers = int.Parse(val);
                            break;
                        case "--scale":
                            benchParameters.Documents = int.Parse(val);
                            break;
                        case "--start":
                            benchParameters.Start = DateTime.Parse(val);
                            break;
                        case "--end":
                            benchParameters.End = DateTime.Parse(val);
                            break;
                        case "--interval":
                            benchParameters.IntervalMs = int.Parse(val);
                            break;
                        case "--metrics":
                            benchParameters.ValuesPerTimeSeries = int.Parse(val);
                            break;
                        case "--jitter":
                            benchParameters.Jitter = int.Parse(val);
                            break;
                        case "--clean":
                            benchParameters.Cleanup = bool.Parse(val);
                            break;
                        default:
                            throw new ArgumentException("Unknown argument " + key);
                    }
                }

                if (string.IsNullOrEmpty(url))
                    throw new ArgumentException("--url must be specified");

                if (workers <= 0)
                    throw new ArgumentException("--workers must be greater than zero");

                if (benchParameters.Start == default)
                    throw new ArgumentException("--start date is undefined");

                if (benchParameters.End == default)
                    throw new ArgumentException("--end date is undefined");

                if (benchParameters.End < benchParameters.Start)
                    throw new ArgumentException("--start date is greater than --end date");

                if (benchParameters.Documents <= 0)
                    throw new ArgumentException("--scale must be greater than zero");

                if (benchParameters.IntervalMs <= 0)
                    throw new ArgumentException("--interval must be greater than zero");

                if (benchParameters.ValuesPerTimeSeries <= 0)
                    throw new ArgumentException("--metrics must be greater than zero");

                return benchParameters;
            }

            private static async Task LocalTest()
            {
                var url = "http://localhost:8080";
                var workers = 8;

                var now = new DateTime(2020, 4, 20);
                var test1 = new BenchParameters
                {
                    Documents = 100,
                    TimeSeriesPerDocument = 1,
                    ValuesPerTimeSeries = 1,
                    Start = now.AddMonths(-1),
                    End = now,
                    IntervalMs = 10_000
                };

                var test2 = new BenchParameters
                {
                    Documents = 100,
                    TimeSeriesPerDocument = 1,
                    ValuesPerTimeSeries = 10,
                    Start = now.AddMonths(-1),
                    End = now,
                    IntervalMs = 10_000
                };

                var test3 = new BenchParameters
                {
                    Documents = 4000,
                    TimeSeriesPerDocument = 1,
                    ValuesPerTimeSeries = 10,
                    Start = now.AddDays(-3),
                    End = now,
                    IntervalMs = 10_000
                };

                var test4 = new BenchParameters
                {
                    Documents = 100_000,
                    TimeSeriesPerDocument = 1,
                    ValuesPerTimeSeries = 10,
                    Start = now.AddHours(-3),
                    End = now,
                    IntervalMs = 10_000
                };

                var test5 = new BenchParameters
                {
                    Documents = 1_000_000,
                    TimeSeriesPerDocument = 1,
                    ValuesPerTimeSeries = 10,
                    Start = now.AddMinutes(-3),
                    End = now,
                    IntervalMs = 10_000
                };

                var tests = new[] { test1, test2, test3, test4, test5 };

                //foreach (var test in tests)
                {
                    using (var tsb = new TimeSeriesBench(url, workers))
                    {
                        tsb.Initialize(test3);

                        await tsb.BenchmarkInsert();
                        await Task.Delay(10_000); // cooldown

                        await tsb.BenchmarkQuery();
                        Console.WriteLine();
                    }
                }
            }
        }

        public class BenchParameters
        {
            public int Documents;
            public int TimeSeriesPerDocument;
            public int ValuesPerTimeSeries;
            public DateTime Start;
            public DateTime End;
            public int IntervalMs;
            public int Jitter;

            public bool Cleanup;

            public override int GetHashCode()
            {
                return (int)(Documents ^ TimeSeriesPerDocument ^ ValuesPerTimeSeries ^ Start.Ticks ^ End.Ticks ^ IntervalMs ^ Jitter); // should be good enough
            }
        }

        private readonly string _url;
        private readonly int _workers;

        private DocumentStore _store;
        private readonly string _workingDir;
        private string _currentDir;
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
        private int _jitter;

        private int TotalTimeSeries => _timeSeriesPerDocument * _numberOfDocs;
        private int TotalMeasuresPerDocument => _timeSeriesPerDocument * _measuresPerTimeSeries * _valuesPerMeasure;
        private long TotalMetrics => TotalMeasuresPerDocument * _numberOfDocs;

        public TimeSeriesBench(string url, int workers, string workingDir = null)
        {
            _url = url;
            _workers = workers;
            _workingDir = workingDir ?? "./TimeSeriesBenchmarkData";
        }

        public void Initialize(BenchParameters parameters)
        {
            _numberOfDocs = parameters.Documents;
            _timeSeriesPerDocument = parameters.TimeSeriesPerDocument;
            _valuesPerMeasure = parameters.ValuesPerTimeSeries;
            _from = parameters.Start;
            _to = parameters.End;
            _interval = parameters.IntervalMs;
            _jitter = parameters.Jitter;
            _measuresPerTimeSeries = (int)((_to - _from).TotalMilliseconds / _interval);

            _names.Clear();
            Log(
                $"Initialize to run with {_numberOfDocs:N0} documents, {_timeSeriesPerDocument:N0} time-series for each, {_valuesPerMeasure} metrics per measure. Total metrics {TotalMetrics:N0}.");

            var testHash = parameters.GetHashCode() ^ _workers;
            _currentDir = Path.Combine(_workingDir, testHash.ToString());

            var databaseName = "TimeSeriesBenchmark_" + parameters.GetHashCode();
            _store = new DocumentStore
            {
                Urls = new[] { _url },
                Database = databaseName,
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            };
            _store.Initialize();

            CreateDb(databaseName, parameters.Cleanup);

            InitializeLocalVariables();
        }

        private DataGenerator GetDataGenerator()
        {
            return new DataGenerator(numberOfValues: _valuesPerMeasure, start: _from, millisecondsInterval: _interval, _jitter);
        }

        public void Arm()
        {
            _onFire = new TaskCompletionSource<bool>();
            _fire = new CountdownEvent(Math.Min(TotalTimeSeries, _workers));
        }

        public Task Fire()
        {
            Task.Run(() =>
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
            [ThreadStatic]
            private static Random _currentThreadRand;

            private Random _rand => _currentThreadRand ??= new Random();

            private long _elapsedMilliseconds;

            private readonly int _numberOfValues;
            private readonly int _millisecondsInterval;
            private readonly bool _withTag;
            private readonly DateTime _baseline;
            private readonly int _timeJitter = 20; // default is a jitter of 20%

            public DataGenerator(int numberOfValues = 1, DateTime? start = null, int millisecondsInterval = 10_000, int jitter = 0)
            {
                _numberOfValues = numberOfValues;
                _millisecondsInterval = millisecondsInterval;
                _withTag = false;
                _baseline = start ?? DateTime.UtcNow;
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
                    m.Values[i] = (double)_rand.Next(0, 1_000) / 10; // 0.0 - 100.0
                }

                if (_withTag)
                {
                    m.Tag = RandomString(10);
                }

                return m;
            }

            private const string _chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890 ";

            private string RandomString(int length)
            {
                var str = new char[length];
                for (int i = 0; i < length; i++)
                {
                    str[i] = _chars[_rand.Next(_chars.Length)];
                }

                return new string(str);
            }

            private readonly string[] _versionChoice = { "v1.0", "v1.5", "v2.0", "v2.3" };
            private readonly string[] _driverChoices = { "Derek", "Rodney", "Albert", "Andy", "Seth", "Trish" };

            private readonly TruckModel[] _modelChoices =
            {
                new TruckModel {Name = "F-150", LoadCapacity = 2000, FuelCapacity = 200, FuelConsumption = 15},
                new TruckModel {Name = "G-2000", LoadCapacity = 5000, FuelCapacity = 300, FuelConsumption = 19},
                new TruckModel {Name = "H-2", LoadCapacity = 1500, FuelCapacity = 150, FuelConsumption = 12}
            };

            public Driver GenerateDriver()
            {
                var rand = _rand;
                return new Driver
                {
                    Name = _driverChoices[rand.Next(0, _driverChoices.Length - 1)],
                    TruckModel = _modelChoices[rand.Next(0, _modelChoices.Length - 1)],
                    Version = _versionChoice[rand.Next(0, _versionChoice.Length - 1)]
                };
            }
        }

        private async Task Worker(int workerIndex)
        {
            string tag = null;
            var path = Path.Combine(_currentDir, $"worker_{workerIndex}.ts-data");

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

        private class TruckModel
        {
            public string Name;
            public int LoadCapacity;
            public int FuelCapacity;
            public int FuelConsumption;
        }

        private class Driver
        {
            public string Id { get; set; }
            public TruckModel TruckModel { get; set; }
            public string Version { get; set; }
            public string Name { get; set; }
        }

        private class DataPreparationParams
        {
            public readonly Dictionary<string, List<string>> Names = new Dictionary<string, List<string>>();
        }

        private DataPreparationParams[] _dataPreparationParams;
        private Driver[] _drivers;

        private async Task CreateDocuments()
        {
            await using (var bulk = _store.BulkInsert())
            {
                foreach (var driver in _drivers)
                {
                    await bulk.StoreAsync(driver);
                }
            }

            Log($"Created {_numberOfDocs} documents");
        }

        public void InitializeLocalVariables()
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
            _drivers = new Driver[_numberOfDocs];
            var generator = new DataGenerator();
            for (int i = 0; i < _numberOfDocs; i++)
            {
                var id = "drivers/" + (i + 1);
                _drivers[i] = generator.GenerateDriver();
                _drivers[i].Id = id;

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
            var fired = Fire();

            var sp = Stopwatch.StartNew();
            await CreateDocuments();

            var tasks = new List<Task>();
            for (int i = 0; i < _workers; i++)
            {
                var t = Worker(i);
                tasks.Add(t);
            }

            await fired;
            await Task.WhenAll(tasks);

            sp.Stop();
            var rate = _numberOfDocs * (TotalMeasuresPerDocument / sp.Elapsed.TotalSeconds);
            var stats = await _store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
            return $"Inserting is completed after {sp.Elapsed} with total rate of {(int)rate:N0} measures per second with {stats.SizeOnDisk.HumaneSize} size.";
        }

        public async Task BenchmarkInsert(int iterations = 1)
        {
            await PrepareData();
            for (int i = 1; i <= iterations; i++)
            {
                Log("Start inserting data...");
                var result = await DoInsert();
                Log(result);
            }
        }

        public async Task BenchmarkQuery(int iterations = 1)
        {
            Log("Start benching query");
            for (int i = 0; i < iterations; i++)
            {
                await SingleGroupBy(1, 1, 1);
                await SingleGroupBy(1, 1, 12);
                await SingleGroupBy(1, 8, 1);
                await DoubleGroupBy(1);

                Console.WriteLine();

                if (_valuesPerMeasure >= 5)
                {
                    await SingleGroupBy(5, 1, 1);
                    await SingleGroupBy(5, 1, 12);
                    await SingleGroupBy(5, 8, 1);
                    await DoubleGroupBy(5);
                }

                if (_valuesPerMeasure >= 10)
                {
                    await DoubleGroupBy(10);
                }
            }
        }

        private async Task SingleGroupBy(int metrics, int series, int hours)
        {
            using (var session = _store.OpenAsyncSession(
                new SessionOptions
                {
                    NoCaching = true
                }))
            {
                var ids = _names.Keys.Take(series);
                var name = _names.First().Value[0];

                var middle = (_to - _from) / 2;

                var to = _from.Add(middle);
                var from = to.AddHours(-hours);

                var query = session.Query<Driver>()
                    .Where(u => u.Id.In(ids))
                    .Statistics(out var stats)
                    .Select(u => RavenQuery.TimeSeries(u, name, from, to)
                        .GroupBy(g => g.Minutes(5))
                        .Select(g => new
                        {
                            Max = g.Max()//.Take(metrics)
                        })
                        .ToList());

                var result = await query.ToListAsync();
                var total = 0;
                foreach (var aggResult in result)
                {
                    total += aggResult.Results.Length;
                }

                Log($"Group by for {metrics} metrics, {series} hosts every 5 minutes for {hours} hours took {stats.DurationInMs} ms (total results: {total})");
            }
        }

        private async Task DoubleGroupBy(int metrics)
        {
            using (var session = _store.OpenAsyncSession(
                new SessionOptions
                {
                    NoCaching = true
                }))
            {
                var middle = (_to - _from) / 2;

                var to = _from.Add(middle);
                var from = to.AddHours(-12);

                var name = _names.First().Value[0];
                var query = session.Query<Driver>()
                    .Statistics(out var stats)
                    .Select(u =>
                        RavenQuery.TimeSeries(u, name, from, to)
                        .GroupBy(g => g.Hours(1))
                        .Select(g => new
                        {
                            Avg = g.Average()//.Take(metrics)
                        })
                        .ToList());

                var result = await query.ToListAsync();
                var total = 0;
                foreach (var aggResult in result)
                {
                    total += aggResult.Results.Length;
                }

                Log($"Double Group by for {metrics} metrics took {stats.DurationInMs} ms (total results: {total})");
            }
        }

        private async Task PrepareData()
        {
            var sp = Stopwatch.StartNew();
            Log($"Start preparing data for {_workers} workers.");

            if (Directory.Exists(_currentDir) == false)
            {
                try
                {
                    Directory.CreateDirectory(_currentDir);

                    for (int i = 0; i < _workers; i++)
                    {
                        await PrepareDataForWorker(i);
                        Log($"Data for worker {i} is ready.");
                    }
                }
                catch (Exception e)
                {
                    try
                    {
                        Directory.Delete(_currentDir);
                    }
                    catch (Exception ex)
                    {
                        throw new AggregateException(e, ex);
                    }

                    throw;
                }
            }

            Log($"Preparation of data is completed after {sp.Elapsed}.");
        }

        private async Task PrepareDataForWorker(int workerIndex)
        {
            var data = _dataPreparationParams[workerIndex];
            try
            {
                var path = Path.Combine(_currentDir, $"worker_{workerIndex}.ts-data");
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

        public void CreateDb(string databaseName, bool shouldCleanup)
        {
            if (Directory.Exists(_workingDir) == false)
                Directory.CreateDirectory(_workingDir);

            try
            {
                var doc = new DatabaseRecord(databaseName);
                _store.Maintenance.Server.Send(new CreateDatabaseOperation(doc));

                if (shouldCleanup)
                {
                    _store.BeforeDispose += (o, e) =>
                    {
                        try
                        {
                            Directory.Delete(_currentDir);
                        }
                        catch (Exception ex)
                        {
                            Log("Failed to delete working dir " + _workingDir + ". Exception : " + ex.Message, true);
                        }

                        try
                        {
                            _store.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName, hardDelete: true));
                        }
                        catch (Exception ex)
                        {
                            Log("Cannot delete DB " + databaseName + ". Exception : " + ex.Message, true);
                        }
                    };
                }
            }
            catch (Exception ex)
            {
                if (ex.Message.Contains("already exists"))
                {
                    Log($"Database '{databaseName}' already exists!");
                }
                else
                {
                    Log("Cannot create DB " + databaseName + ". Exception : " + ex.Message, true);
                    throw;
                }
            }
        }

        public void Dispose()
        {
            _store.Dispose();
        }
    }
}
