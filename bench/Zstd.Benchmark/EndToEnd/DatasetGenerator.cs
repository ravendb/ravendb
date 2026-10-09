using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Sparrow.Utils;
using Zstd.Benchmark.Infrastructure;

namespace Zstd.Benchmark.EndToEnd
{
    internal sealed class DatasetInfo
    {
        public int GeneratorVersion { get; set; }
        public double RequestedGiB { get; set; }
        public int Variants { get; set; }
        public long Documents { get; set; }
        public long JsonBytes { get; set; }
    }

    /// <summary>
    /// The benchmark database: copies of the Northwind documents with mutated values and ids (DocumentMutator, references stay
    /// consistent), generated as a smuggler dump and imported once. Reused across runs while it matches the requested size.
    /// </summary>
    internal static class DatasetGenerator
    {
        public const string DatabaseName = "zstd-e2e";
        private const int Version = 1;

        // the ids DocumentMutator shifts per variant; others (e.g. Raven/Hilo/orders) would be the same in every variant
        private static readonly Regex VariantId = new(@"^[A-Za-z]+/\d+(-[A-Z])?$", RegexOptions.Compiled);

        public static async Task<DatasetInfo> EnsureAsync(IDocumentStore store, string workDirectory, double gib)
        {
            string markerPath = Path.Combine(workDirectory, "dataset.json");
            DatasetInfo existing = File.Exists(markerPath) ? JsonSerializer.Deserialize<DatasetInfo>(File.ReadAllText(markerPath)) : null;
            string[] databases = await store.Maintenance.Server.SendAsync(new GetDatabaseNamesOperation(0, int.MaxValue));
            if (existing != null && existing.GeneratorVersion == Version && Math.Abs(existing.RequestedGiB - gib) < 0.001 && databases.Contains(DatabaseName))
            {
                DatabaseStatistics stats = await store.Maintenance.ForDatabase(DatabaseName).SendAsync(new GetStatisticsOperation());
                if (stats.CountOfDocuments == existing.Documents)
                {
                    Console.WriteLine($"// [e2e] reusing database '{DatabaseName}': {existing.Documents:N0} documents, {existing.JsonBytes / (double)(1L << 30):F2} GiB of JSON");
                    return existing;
                }
            }

            if (databases.Contains(DatabaseName))
                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(DatabaseName, hardDelete: true, timeToWaitForConfirmation: TimeSpan.FromMinutes(5)));

            string dumpPath = Path.Combine(workDirectory, "dataset.ravendbdump");
            BuildNumber build = await store.Maintenance.Server.SendAsync(new GetBuildNumberOperation());
            DatasetInfo info = Generate(dumpPath, gib, build.BuildVersion);

            await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(DatabaseName)));
            Stopwatch sw = Stopwatch.StartNew();
            Raven.Client.Documents.Operations.Operation import = await store.Smuggler.ForDatabase(DatabaseName)
                .ImportAsync(new DatabaseSmugglerImportOptions { OperateOnTypes = DatabaseItemType.Documents }, dumpPath);
            await import.WaitForCompletionAsync(TimeSpan.FromHours(2));

            DatabaseStatistics imported = await store.Maintenance.ForDatabase(DatabaseName).SendAsync(new GetStatisticsOperation());
            if (imported.CountOfDocuments != info.Documents)
                throw new InvalidOperationException($"Expected {info.Documents:N0} documents after the import, got {imported.CountOfDocuments:N0}");

            Console.WriteLine($"// [e2e] imported {info.Documents:N0} documents in {sw.Elapsed.TotalSeconds:F0}s");
            File.WriteAllText(markerPath, JsonSerializer.Serialize(info, new JsonSerializerOptions { WriteIndented = true }));
            File.Delete(dumpPath);
            return info;
        }

        private static DatasetInfo Generate(string dumpPath, double gib, int buildVersion)
        {
            List<string> sources = SourceData.NorthwindDocuments.Select(KeepIdAndCollection).Where(x => x != null).ToList();
            long target = (long)(gib * (1L << 30));
            int variantBytes = BuildVariant(sources, 1).Length;
            int variants = (int)Math.Max(1, (target + variantBytes - 1) / variantBytes);

            Console.WriteLine($"// [e2e] generating {variants:N0} variants x {sources.Count:N0} documents (~{variantBytes / 1024.0:F0} KB each) into {dumpPath}");
            Stopwatch sw = Stopwatch.StartNew();
            long jsonBytes = 0;
            using (BlockingCollection<byte[]> queue = new(boundedCapacity: 64))
            {
                Task producer = Task.Run(() =>
                {
                    try
                    {
                        ParallelOptions parallel = new() { MaxDegreeOfParallelism = Math.Max(1, Environment.ProcessorCount - 2) };
                        Parallel.For(1, variants + 1, parallel, variant => queue.Add(BuildVariant(sources, variant)));
                    }
                    finally
                    {
                        queue.CompleteAdding();
                    }
                });

                using (FileStream file = File.Create(dumpPath))
                using (ZstdStream zstd = ZstdStream.Compress(file, CompressionLevel.Fastest, leaveOpen: false, workers: 4))
                {
                    // the build version of the server the dump is imported into, so smuggler applies no conversions
                    Write(zstd, $"{{\"BuildVersion\":{buildVersion},\"Docs\":[");
                    bool first = true;
                    foreach (byte[] chunk in queue.GetConsumingEnumerable())
                    {
                        if (first == false)
                            Write(zstd, ",");
                        first = false;
                        zstd.Write(chunk);
                        jsonBytes += chunk.Length;
                    }

                    Write(zstd, "]}");
                }

                producer.GetAwaiter().GetResult();
            }

            Console.WriteLine($"// [e2e] generated {jsonBytes / (double)(1L << 30):F2} GiB of JSON in {sw.Elapsed.TotalSeconds:F0}s");
            return new DatasetInfo
            {
                GeneratorVersion = Version,
                RequestedGiB = gib,
                Variants = variants,
                Documents = (long)variants * sources.Count,
                JsonBytes = jsonBytes
            };
        }

        /// <summary>
        /// Drops change vectors, flags, attachments and counters from the metadata, the generated documents have none of those.
        /// Returns null for documents whose id would not be unique per variant.
        /// </summary>
        private static string KeepIdAndCollection(string json)
        {
            JsonObject document = JsonNode.Parse(json).AsObject();
            JsonObject metadata = document["@metadata"].AsObject();
            string id = metadata["@id"].GetValue<string>();
            if (VariantId.IsMatch(id) == false)
                return null;

            document["@metadata"] = new JsonObject
            {
                ["@collection"] = metadata["@collection"].GetValue<string>(),
                ["@id"] = id
            };
            return document.ToJsonString();
        }

        /// <summary>
        /// Variant n shifts every id (and every reference to it) by n * 100,000, so variants never overwrite each other.
        /// </summary>
        private static byte[] BuildVariant(List<string> sources, int variant)
        {
            StringBuilder sb = new();
            for (int i = 0; i < sources.Count; i++)
            {
                if (i > 0)
                    sb.Append(',');
                sb.Append(DocumentMutator.Mutate(sources[i], variant, seed: i));
            }

            return Encoding.UTF8.GetBytes(sb.ToString());
        }

        private static void Write(Stream stream, string text) => stream.Write(Encoding.UTF8.GetBytes(text));
    }
}
