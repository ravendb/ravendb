using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Sparrow.Json;
using Sparrow.Json.Sync;

namespace Zstd.Benchmark.Infrastructure
{
    public enum PayloadKind
    {
        /// <summary>Smuggler export / logical backup / HTTP body: JSON text (plus binary attachments for the dump part).</summary>
        Json,

        /// <summary>Documents in their storage representation, closer to what snapshot backups and Voron pages contain.</summary>
        Blittable
    }

    /// <summary>
    /// Documents of one collection, in the representation Voron compresses (blittable), split the way production uses them:
    /// a dictionary is trained on recent documents and then used for documents written afterwards.
    /// </summary>
    internal sealed class DocumentCorpus
    {
        public string Name;
        public byte[][] TrainingDocuments;
        public byte[][] Documents;
        public long DocumentsBytes;

        public double AverageDocumentSize => (double)DocumentsBytes / Documents.Length;
    }

    internal static class Datasets
    {
        /// <summary>Documents processed per benchmark invocation (fixed, so BenchmarkDotNet can report per-document cost).</summary>
        public const int DocumentsPerCorpus = 1024;

        /// <summary>Bump when the generated data changes so stale disk caches are not reused.</summary>
        private const string GeneratorVersion = "v1";

        public static readonly string[] DocumentDatasets = { "Orders", "Companies", "CompanyWithOrders", "Monsters" };

        private static readonly ConcurrentDictionary<string, Lazy<DocumentCorpus>> Corpora = new();
        private static readonly ConcurrentDictionary<(PayloadKind, int), Lazy<byte[]>> Payloads = new();

        public static DocumentCorpus GetCorpus(string dataset) =>
            Corpora.GetOrAdd(dataset, name => new Lazy<DocumentCorpus>(() => BuildCorpus(name))).Value;

        /// <summary>
        /// Training documents come from the first half of the source documents, benchmarked documents from the second half,
        /// so the dictionary never saw the exact documents it compresses. Both halves are grown with mutated variants when needed.
        /// </summary>
        private static DocumentCorpus BuildCorpus(string dataset)
        {
            IReadOnlyList<string> source = SourceData.GetDocuments(dataset);
            int half = source.Count / 2;

            List<string> training = Expand(source.Take(half).ToList(), DictionaryTrainer.MaxSamples);
            List<string> documents = Expand(source.Skip(half).ToList(), DocumentsPerCorpus);

            DocumentCorpus corpus = new()
            {
                Name = dataset,
                TrainingDocuments = ToBlittable(training),
                Documents = ToBlittable(documents)
            };
            corpus.DocumentsBytes = corpus.Documents.Sum(x => (long)x.Length);
            return corpus;
        }

        private static List<string> Expand(List<string> source, int count)
        {
            List<string> results = new(count);
            for (int variant = 0; results.Count < count; variant++)
            {
                for (int i = 0; i < source.Count && results.Count < count; i++)
                    results.Add(DocumentMutator.Mutate(source[i], variant, seed: i));
            }

            return results;
        }

        public static byte[][] ToBlittable(IReadOnlyList<string> documents)
        {
            byte[][] results = new byte[documents.Count][];
            JsonOperationContext context = JsonOperationContext.ShortTermSingleUse();
            try
            {
                for (int i = 0; i < documents.Count; i++)
                {
                    if (i % 512 == 511)
                    {
                        context.Dispose();
                        context = JsonOperationContext.ShortTermSingleUse();
                    }

                    using (BlittableJsonReaderObject blittable = context.Sync.ReadForMemory(documents[i], "zstd-bench/" + i))
                        results[i] = blittable.AsSpan().ToArray();
                }
            }
            finally
            {
                context.Dispose();
            }

            return results;
        }

        /// <summary>
        /// Stream payload of an exact size. Json: the real Northwind export (documents, revisions, attachments) followed by mutated
        /// copies of its documents. Blittable: the same documents in storage form, concatenated. Cached on disk across processes.
        /// </summary>
        public static byte[] GetPayload(PayloadKind kind, int size) =>
            Payloads.GetOrAdd((kind, size), key => new Lazy<byte[]>(() => LoadOrCreatePayload(key.Item1, key.Item2))).Value;

        public static string CacheDirectory => Path.Combine(Path.GetTempPath(), "ravendb-zstd-bench", GeneratorVersion);

        private static byte[] LoadOrCreatePayload(PayloadKind kind, int size)
        {
            string directory = CacheDirectory;
            string path = Path.Combine(directory, $"{kind}-{size}.bin");
            if (File.Exists(path) && new FileInfo(path).Length == size)
                return File.ReadAllBytes(path);

            byte[] payload = kind == PayloadKind.Json ? CreateJsonPayload(size) : CreateBlittablePayload(size);

            Directory.CreateDirectory(directory);
            string temp = path + "." + Environment.ProcessId + ".tmp";
            File.WriteAllBytes(temp, payload);
            File.Move(temp, path, overwrite: true);
            return payload;
        }

        private static byte[] CreateJsonPayload(int size)
        {
            byte[] payload = new byte[size];
            byte[] dump = SourceData.NorthwindDump;
            int written = Math.Min(dump.Length, size);
            dump.AsSpan(0, written).CopyTo(payload);

            IReadOnlyList<string> documents = SourceData.NorthwindDocuments;
            for (int variant = 1; written < size; variant++)
            {
                for (int i = 0; i < documents.Count && written < size; i++)
                {
                    byte[] bytes = Encoding.UTF8.GetBytes(DocumentMutator.Mutate(documents[i], variant, seed: i) + ",");
                    int toCopy = Math.Min(bytes.Length, size - written);
                    bytes.AsSpan(0, toCopy).CopyTo(payload.AsSpan(written));
                    written += toCopy;
                }
            }

            return payload;
        }

        private static byte[] CreateBlittablePayload(int size)
        {
            byte[] payload = new byte[size];
            IReadOnlyList<string> documents = SourceData.NorthwindDocuments;
            int written = 0;
            for (int variant = 0; written < size; variant++)
            {
                List<string> batch = new(documents.Count);
                for (int i = 0; i < documents.Count; i++)
                    batch.Add(DocumentMutator.Mutate(documents[i], variant, seed: i));

                foreach (byte[] blittable in ToBlittable(batch))
                {
                    int toCopy = Math.Min(blittable.Length, size - written);
                    blittable.AsSpan(0, toCopy).CopyTo(payload.AsSpan(written));
                    written += toCopy;
                    if (written == size)
                        break;
                }
            }

            return payload;
        }
    }
}
