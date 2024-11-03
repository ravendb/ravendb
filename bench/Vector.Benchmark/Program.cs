using System.Diagnostics;
using System.Runtime.InteropServices;
using Parquet;
using Voron;
using Voron.Data.Graphs;

string[] Files = [
                 "train-00000-of-00004-1a1932c9ca1c7152.parquet",
                 "train-00001-of-00004-f4a4f5540ade14b4.parquet",
                 "train-00002-of-00004-ff770df3ab420d14.parquet",
                 "train-00003-of-00004-85b3dbbc960e92ec.parquet"
             ];
if (Directory.Exists("vectors"))
{
    Directory.Delete("vectors", true);
}
var options = StorageEnvironmentOptions.ForPathForTests("vectors");
using var env = new StorageEnvironment(options);


var sp = Stopwatch.StartNew();
//var id = 16416;
var id = await ImportData(env);
Console.WriteLine(sp.Elapsed);
sp.Restart();
await TestRecall(env, id);
Console.WriteLine(sp.Elapsed);

async Task TestRecall(StorageEnvironment env, long id)
{
    int vectorsCount = 0;
    int correctCount = 0;
    int resultsCount = 8;
    int queries = 50;
    var annMatches = new long[resultsCount];
    var ennMatches = new long[resultsCount];

    Span<float> vector = new float[768];
    var rnd = new Random(454);
    for (int i = 0; i < queries; i++)
    {
        rnd.NextBytes(MemoryMarshal.AsBytes(vector));
        using (var txr = env.ReadTransaction())
        {
            var sp = Stopwatch.StartNew();
            using var enn = Hnsw.ExactNearest(txr.LowLevelTransaction, id, 10, MemoryMarshal.AsBytes(vector));
            var exactSp = sp.Elapsed;
            sp.Restart();
            using var ann = Hnsw.ApproximateNearest(txr.LowLevelTransaction, id, 10, MemoryMarshal.AsBytes(vector));
            var approxSp = sp.Elapsed;
            Console.WriteLine($"Exact {exactSp.TotalMilliseconds} - Approximate {approxSp.TotalMilliseconds} -" +
                $" diff: {Math.Round(exactSp.TotalMilliseconds - approxSp.TotalMilliseconds, 4)} ms");

            vectorsCount++;

            var aRead = ann.Fill(annMatches);
            var eRead = enn.Fill(ennMatches);
            if (aRead != eRead)
            {
                Console.WriteLine("Mismatch in read count?");
            }
            foreach (var annMatch in annMatches)
            {
                if (ennMatches.Contains(annMatch))
                {
                    correctCount++;
                }
            }
        }

    }
    Console.WriteLine($"{correctCount} - {queries*resultsCount} {correctCount / (float)(queries * resultsCount):P} matches");
}

async Task<long> ImportData(StorageEnvironment env)
{
    long id;
    using (var txw = env.WriteTransaction())
    {
        id = Hnsw.Create(txw.LowLevelTransaction, 768 * 4, 8, 32);
        Console.WriteLine(id);
        txw.Commit();
    }

    foreach (var file in Files)
    {
        var fullPath = Path.Combine(Path.GetTempPath(), "Vector.Benchmark", file);
        if (File.Exists(fullPath) is false)
        {
            await DownloadFile(fullPath);
        }

        await foreach (var (ids, vectors) in YieldVectors(fullPath))
        {
            var batch = Stopwatch.StartNew();
            using (var txw = env.WriteTransaction())
            {
                using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, id))
                {
                    for (int i = 0; i < ids.Length; i++)
                    {
                        var vector = new Memory<float>(vectors, i * 768, 768);
                        registration.Register(ids[i] * 100, MemoryMarshal.Cast<float, byte>(vector.Span));
                    }
                }
                txw.Commit();
            }
            Console.WriteLine($" * {ids.Length:N0} - {batch.Elapsed}");
        }
    }
    return id;
}


static async IAsyncEnumerable<(int[], float[])> YieldVectors(string filePath)
{
    var file = await ParquetReader.CreateAsync(filePath);
    var schema = file.Schema;
    for (int i = 0; i < file.RowGroupCount; i++)
    {
        var reader = file.OpenRowGroupReader(i);
        var wikiId = await reader.ReadColumnAsync(schema.DataFields[4]);
        var vectors = await reader.ReadColumnAsync(schema.DataFields[8]);
        var wikiIds = (int[])wikiId.DefinedData;
        var vectorsArr = (float[])vectors.DefinedData;
        yield return (wikiIds, vectorsArr);
    }
}

static async Task DownloadFile(string fullPath)
{
    const string url = "https://huggingface.co/datasets/Cohere/wikipedia-22-12-simple-embeddings/resolve/main/data/";

    using (HttpClient client = new())
    {
        client.Timeout = TimeSpan.FromHours(1); // Set timeout to a reasonable value for large files

        using (var response = await client.GetAsync(url + Path.GetFileName(fullPath), HttpCompletionOption.ResponseHeadersRead))
        {
            response.EnsureSuccessStatusCode();

            await using (var contentStream = await response.Content.ReadAsStreamAsync())
            await using (var fileStream = File.Create(fullPath))
            {
                await contentStream.CopyToAsync(fileStream);
            }
        }
    }
}
