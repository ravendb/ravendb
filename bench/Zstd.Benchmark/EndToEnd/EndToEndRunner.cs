using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Zstd.Benchmark.Infrastructure;
using Zstd.Benchmark.Reporting;

namespace Zstd.Benchmark.EndToEnd
{
    internal sealed class EndToEndOptions
    {
        public string ServerPath { get; set; }
        public string WorkDirectory { get; set; }
        public string ResultsDirectory { get; set; }
        public double DatasetGiB { get; set; } = 3;
        public List<int> Workers { get; } = new();
        public List<CompressionLevel> Levels { get; } = new();
        public int Rounds { get; set; } = 2;
        public int Iterations { get; set; } = 3;
        public bool RestoreAndImport { get; set; } = true;
        public string Affinity { get; set; } = "pcores";
        public int Port { get; set; } = 8095;
    }

    internal sealed class EndToEndMeasurement
    {
        public int Round { get; set; }
        public int Iteration { get; set; }
        public string Operation { get; set; }
        public string Level { get; set; }
        public int Workers { get; set; }
        public double Seconds { get; set; }
        public double ServerCpuSeconds { get; set; }
        public long ServerPrivateBytesBefore { get; set; }
        public long ServerPeakPrivateBytes { get; set; }
        public long OutputBytes { get; set; }
    }

    internal sealed class EndToEndRun
    {
        public string Started { get; set; }
        public string Machine { get; set; }
        public string ServerAffinity { get; set; }
        public string License { get; set; }
        public DatasetInfo Dataset { get; set; }
        public int Iterations { get; set; }
        public int Rounds { get; set; }
        public List<EndToEndMeasurement> Measurements { get; set; } = new();
    }

    /// <summary>
    /// Export, logical backup and snapshot backup of the same database through a real server process, with the zstd worker
    /// count and compression level set server-wide. Every configuration gets a fresh server; rounds run the configurations in
    /// alternating order so drift (thermal, background activity) does not favor one of them.
    /// </summary>
    internal static class EndToEndRunner
    {
        public const string Export = "Export";
        public const string Backup = "Backup";
        public const string Snapshot = "Snapshot";
        public const string Restore = "Restore";
        public const string Import = "Import";

        private static readonly string[] WorkerKeys = { "Backup.Compression.Zstd.Workers", "Export.Compression.Zstd.Workers" };
        private static readonly string[] LevelKeys = { "Backup.Compression.Level", "Backup.Snapshot.Compression.Level", "Export.Compression.Level" };

        public static async Task<int> RunAsync(EndToEndOptions options)
        {
            Console.OutputEncoding = Encoding.UTF8;
            string work = Path.GetFullPath(options.WorkDirectory);
            string outputs = Path.Combine(work, "outputs");
            string results = Path.GetFullPath(options.ResultsDirectory ?? Path.Combine(work, "results-" + DateTime.Now.ToString("yyyyMMdd-HHmmss", CultureInfo.InvariantCulture)));
            Directory.CreateDirectory(work);
            Directory.CreateDirectory(results);
            string serverLog = Path.Combine(results, "server.log");

            IntPtr? serverAffinity = ResolveAffinity(options.Affinity, out string affinityDescription);
            if (serverAffinity != null && OperatingSystem.IsWindows())
            {
                // the client only receives the already compressed export, keep it off the server's cores
                long all = (long)Process.GetCurrentProcess().ProcessorAffinity;
                long rest = all & ~(long)serverAffinity.Value;
                if (rest != 0)
                    Process.GetCurrentProcess().ProcessorAffinity = (IntPtr)rest;
            }

            EndToEndRun run = new()
            {
                Started = DateTime.Now.ToString("O", CultureInfo.InvariantCulture),
                Machine = $"{Environment.MachineName}, {Environment.ProcessorCount} logical processors, {Environment.OSVersion}",
                ServerAffinity = affinityDescription,
                Iterations = options.Iterations,
                Rounds = options.Rounds
            };
            Console.WriteLine($"// [e2e] server affinity: {affinityDescription}");
            Console.WriteLine($"// [e2e] results: {results}");

            await using (RavenServerProcess server = await StartServerAsync(options, work, serverLog, workers: 0, CompressionLevel.Fastest, serverAffinity))
            using (IDocumentStore store = CreateStore(server.Url))
            {
                run.Dataset = await DatasetGenerator.EnsureAsync(store, work, options.DatasetGiB);
                run.License = await server.GetLicenseAsync();
                Console.WriteLine($"// [e2e] license: {run.License}");
            }

            List<(CompressionLevel Level, int Workers)> configurations = options.Levels.SelectMany(l => options.Workers.Select(w => (l, w))).ToList();
            for (int round = 1; round <= options.Rounds; round++)
            {
                IEnumerable<(CompressionLevel Level, int Workers)> order = round % 2 == 1 ? configurations : Enumerable.Reverse(configurations);
                foreach ((CompressionLevel level, int workers) in order)
                {
                    Console.WriteLine($"// [e2e] round {round}: level {level}, {workers} workers");
                    await RunConfigurationAsync(options, run, round, level, workers, work, outputs, serverLog, serverAffinity);
                    File.WriteAllText(Path.Combine(results, "measurements.json"), JsonSerializer.Serialize(run, new JsonSerializerOptions { WriteIndented = true }));
                }
            }

            string report = EndToEndReport.Create(run);
            File.WriteAllText(Path.Combine(results, "report.md"), report);
            Console.WriteLine(report);
            return 0;
        }

        private static async Task RunConfigurationAsync(EndToEndOptions options, EndToEndRun run, int round, CompressionLevel level, int workers,
            string work, string outputs, string serverLog, IntPtr? serverAffinity)
        {
            await using RavenServerProcess server = await StartServerAsync(options, work, serverLog, workers, level, serverAffinity);
            using IDocumentStore store = CreateStore(server.Url);

            Dictionary<string, string> values = await server.GetServerValuesAsync(WorkerKeys.Concat(LevelKeys));
            foreach (string key in WorkerKeys)
                Expect(values, key, workers.ToString(CultureInfo.InvariantCulture));
            foreach (string key in LevelKeys)
                Expect(values, key, level.ToString());

            string exportFile = Path.Combine(outputs, "export.ravendbdump");
            string backupDirectory = Path.Combine(outputs, "backup");
            string snapshotDirectory = Path.Combine(outputs, "snapshot");

            // an unmeasured export loads the database and brings its data files into the page cache, so every measured operation starts warm
            await ExportAsync(store, exportFile);

            for (int iteration = 1; iteration <= options.Iterations; iteration++)
            {
                Record(run, round, iteration, level, workers, Export, await MeasureAsync(server, () => ExportAsync(store, exportFile)));
                Record(run, round, iteration, level, workers, Backup, await MeasureAsync(server, () => BackupAsync(store, BackupType.Backup, backupDirectory)));
                Record(run, round, iteration, level, workers, Snapshot, await MeasureAsync(server, () => BackupAsync(store, BackupType.Snapshot, snapshotDirectory)));
            }

            if (options.RestoreAndImport && level == CompressionLevel.Fastest)
            {
                long documents = run.Dataset.Documents;
                string restored = DatasetGenerator.DatabaseName + "-restored";
                await DeleteDatabaseAsync(store, restored);
                Record(run, round, 1, level, workers, Restore, await MeasureAsync(server, () => RestoreAsync(store, backupDirectory, restored, documents)));
                await DeleteDatabaseAsync(store, restored);

                string imported = DatasetGenerator.DatabaseName + "-imported";
                await DeleteDatabaseAsync(store, imported);
                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(imported)));
                Record(run, round, 1, level, workers, Import, await MeasureAsync(server, () => ImportAsync(store, exportFile, imported, documents)));
                await DeleteDatabaseAsync(store, imported);
            }

            if (server.AffinityResets > 0)
                Console.WriteLine($"// [e2e] server affinity was reset {server.AffinityResets} time(s) and pinned again");
        }

        private static void Record(EndToEndRun run, int round, int iteration, CompressionLevel level, int workers, string operation, EndToEndMeasurement measurement)
        {
            measurement.Round = round;
            measurement.Iteration = iteration;
            measurement.Level = level.ToString();
            measurement.Workers = workers;
            measurement.Operation = operation;
            run.Measurements.Add(measurement);
            Console.WriteLine($"// [e2e]   {operation,-8} #{iteration}: {measurement.Seconds,7:F2}s, server CPU {measurement.ServerCpuSeconds,7:F2}s, " +
                              $"peak +{(measurement.ServerPeakPrivateBytes - measurement.ServerPrivateBytesBefore) / 1048576.0,6:F0} MB" +
                              (measurement.OutputBytes > 0 ? $", output {measurement.OutputBytes / 1048576.0,7:F1} MB" : string.Empty));
        }

        private static async Task<EndToEndMeasurement> MeasureAsync(RavenServerProcess server, Func<Task<(long OutputBytes, long EndTimestamp)>> operation)
        {
            server.EnsureAffinity();
            (TimeSpan cpuBefore, long privateBefore) = server.Snapshot();
            long start;
            (long OutputBytes, long EndTimestamp) result;
            long peak;
            await using (PeakMemorySampler sampler = new(server.ProcessId, TimeSpan.FromMilliseconds(50)))
            {
                start = Stopwatch.GetTimestamp();
                result = await operation();
                peak = sampler.PeakPrivateBytes;
            }

            (TimeSpan cpuAfter, _) = server.Snapshot();
            return new EndToEndMeasurement
            {
                Seconds = Stopwatch.GetElapsedTime(start, result.EndTimestamp).TotalSeconds,
                ServerCpuSeconds = (cpuAfter - cpuBefore).TotalSeconds,
                ServerPrivateBytesBefore = privateBefore,
                ServerPeakPrivateBytes = Math.Max(peak, privateBefore),
                OutputBytes = result.OutputBytes
            };
        }

        private static async Task<(long, long)> ExportAsync(IDocumentStore store, string file)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(file));
            File.Delete(file);
            await using TimestampedFileStream stream = new(file);
            Operation operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions { OperateOnTypes = DatabaseItemType.Documents }, stream);
            await operation.WaitForCompletionAsync(TimeSpan.FromHours(1));
            long completed = Stopwatch.GetTimestamp();

            // the operation completes when the server is done, the client may still be writing the tail of the response
            while (Stopwatch.GetElapsedTime(stream.LastWriteTimestamp) < TimeSpan.FromMilliseconds(500))
                await Task.Delay(100);

            return (stream.Length, Math.Max(completed, stream.LastWriteTimestamp));
        }

        private static async Task<(long, long)> BackupAsync(IDocumentStore store, BackupType type, string directory)
        {
            DeleteDirectory(directory);
            Operation operation = await store.Maintenance.SendAsync(new BackupOperation(new BackupConfiguration
            {
                BackupType = type,
                LocalSettings = new LocalSettings { FolderPath = directory }
            }));
            await operation.WaitForCompletionAsync(TimeSpan.FromHours(1));
            long end = Stopwatch.GetTimestamp();
            return (DirectorySize(directory), end);
        }

        private static async Task<(long, long)> RestoreAsync(IDocumentStore store, string backupDirectory, string databaseName, long expectedDocuments)
        {
            Operation operation = await store.Maintenance.Server.SendAsync(new RestoreBackupOperation(new RestoreBackupConfiguration
            {
                BackupLocation = Directory.GetDirectories(backupDirectory).Single(),
                DatabaseName = databaseName
            }));
            await operation.WaitForCompletionAsync(TimeSpan.FromHours(1));
            long end = Stopwatch.GetTimestamp();
            await AssertDocumentsAsync(store, databaseName, expectedDocuments);
            return (0, end);
        }

        private static async Task<(long, long)> ImportAsync(IDocumentStore store, string file, string databaseName, long expectedDocuments)
        {
            Operation operation = await store.Smuggler.ForDatabase(databaseName)
                .ImportAsync(new DatabaseSmugglerImportOptions { OperateOnTypes = DatabaseItemType.Documents }, file);
            await operation.WaitForCompletionAsync(TimeSpan.FromHours(1));
            long end = Stopwatch.GetTimestamp();
            await AssertDocumentsAsync(store, databaseName, expectedDocuments);
            return (0, end);
        }

        private static async Task AssertDocumentsAsync(IDocumentStore store, string databaseName, long expected)
        {
            DatabaseStatistics stats = await store.Maintenance.ForDatabase(databaseName).SendAsync(new GetStatisticsOperation());
            if (stats.CountOfDocuments != expected)
                throw new InvalidOperationException($"'{databaseName}' has {stats.CountOfDocuments:N0} documents, expected {expected:N0}");
        }

        private static async Task DeleteDatabaseAsync(IDocumentStore store, string databaseName)
        {
            string[] existing = await store.Maintenance.Server.SendAsync(new GetDatabaseNamesOperation(0, int.MaxValue));
            if (existing.Contains(databaseName) == false)
                return;

            await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(databaseName, hardDelete: true, timeToWaitForConfirmation: TimeSpan.FromMinutes(5)));
            Stopwatch sw = Stopwatch.StartNew();
            while (sw.Elapsed < TimeSpan.FromMinutes(5))
            {
                string[] names = await store.Maintenance.Server.SendAsync(new GetDatabaseNamesOperation(0, int.MaxValue));
                if (names.Contains(databaseName) == false)
                    break;
                await Task.Delay(500);
            }

            // let the deletion of the data files settle before the next measurement
            await Task.Delay(TimeSpan.FromSeconds(5));
        }

        private static async Task<RavenServerProcess> StartServerAsync(EndToEndOptions options, string work, string serverLog, int workers, CompressionLevel level, IntPtr? affinity)
        {
            Dictionary<string, string> settings = new();
            foreach (string key in WorkerKeys)
                settings[key] = workers.ToString(CultureInfo.InvariantCulture);
            foreach (string key in LevelKeys)
                settings[key] = level.ToString();

            return await RavenServerProcess.StartAsync(Path.GetFullPath(options.ServerPath), Path.Combine(work, "server-data"), serverLog, options.Port, settings, affinity);
        }

        private static IDocumentStore CreateStore(string url)
        {
            DocumentStore store = new() { Urls = new[] { url }, Database = DatasetGenerator.DatabaseName };
            store.Conventions.DisableTopologyUpdates = true;
            // the export is already zstd compressed, keep HTTP compression out of the measurement
            store.Conventions.UseHttpCompression = false;
            store.Conventions.UseHttpDecompression = false;
            return store.Initialize();
        }

        private static void Expect(Dictionary<string, string> values, string key, string expected)
        {
            if (values.TryGetValue(key, out string actual) == false || string.Equals(actual, expected, StringComparison.OrdinalIgnoreCase) == false)
                throw new InvalidOperationException($"The server runs with {key}={actual ?? "<default>"}, expected {expected}");
        }

        private static IntPtr? ResolveAffinity(string affinity, out string description)
        {
            switch (affinity)
            {
                case "none":
                    description = "none";
                    return null;
                case "pcores":
                    return CpuTopology.GetPerformanceCoresMask(out description);
                default:
                    long mask = long.Parse(affinity.Replace("0x", string.Empty), NumberStyles.HexNumber, CultureInfo.InvariantCulture);
                    description = $"mask 0x{mask:X}";
                    return (IntPtr)mask;
            }
        }

        private static long DirectorySize(string directory) =>
            Directory.Exists(directory) ? Directory.EnumerateFiles(directory, "*", SearchOption.AllDirectories).Sum(f => new FileInfo(f).Length) : 0;

        private static void DeleteDirectory(string directory)
        {
            for (int attempt = 0; Directory.Exists(directory); attempt++)
            {
                try
                {
                    Directory.Delete(directory, recursive: true);
                }
                catch (IOException) when (attempt < 10)
                {
                    Thread.Sleep(500);
                }
            }
        }

        /// <summary>
        /// Remembers when the last bytes of the export arrived.
        /// </summary>
        private sealed class TimestampedFileStream : FileStream
        {
            public TimestampedFileStream(string path) : base(path, FileMode.Create, FileAccess.Write, FileShare.Read, 1024 * 1024, useAsync: true)
            {
                LastWriteTimestamp = Stopwatch.GetTimestamp();
            }

            public long LastWriteTimestamp { get; private set; }

            public override void Write(byte[] buffer, int offset, int count)
            {
                base.Write(buffer, offset, count);
                LastWriteTimestamp = Stopwatch.GetTimestamp();
            }

            public override void Write(ReadOnlySpan<byte> buffer)
            {
                base.Write(buffer);
                LastWriteTimestamp = Stopwatch.GetTimestamp();
            }

            public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                await base.WriteAsync(buffer, offset, count, cancellationToken);
                LastWriteTimestamp = Stopwatch.GetTimestamp();
            }

            public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            {
                await base.WriteAsync(buffer, cancellationToken);
                LastWriteTimestamp = Stopwatch.GetTimestamp();
            }
        }
    }
}
