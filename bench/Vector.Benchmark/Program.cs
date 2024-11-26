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
var dbPath = Path.GetFullPath("vectors");
if (Directory.Exists(dbPath))
{
    Directory.Delete(dbPath, true);
}

var sp = Stopwatch.StartNew();
await ImportData(dbPath);
Console.WriteLine(sp.Elapsed);
sp.Restart();
TestRecall(dbPath);
Console.WriteLine(sp.Elapsed);

void TestRecall(string path)
{
    var options = StorageEnvironmentOptions.ForPathForTests(path);
    using var env = new StorageEnvironment(options);

    int correctCount = 0;
    int resultsCount = 8;
    int queries = 50;
    var annMatches = new long[resultsCount];
    var annDistances = new float[resultsCount];
    var ennMatches = new long[resultsCount];
    var ennDistances = new float[resultsCount];

    long results = 0;
    foreach (var file in Files)
    {
        using var txr = env.ReadTransaction();
        var fullPath = Path.Combine(Path.GetTempPath(), "Vector.Benchmark", file);

        foreach (var (ids, vectors) in YieldVectors(fullPath))
        {
            for (int i = 0; i < ids.Length; i++)
            {
                var vector = new Span<float>(vectors, i * 768, 768);
                
                using var enn = Hnsw.ExactNearest(txr.LowLevelTransaction, "wiki", 10, MemoryMarshal.AsBytes(vector));
                using var ann = Hnsw.ApproximateNearest(txr.LowLevelTransaction, "wiki", 64, MemoryMarshal.AsBytes(vector));

                var aRead = ann.Fill(annMatches, annDistances);
                var eRead = enn.Fill(ennMatches, ennDistances);
                if (aRead != eRead)
                {
                    Console.WriteLine("Mismatch in read count?");
                }

                results += aRead;
                foreach (var annMatch in annMatches)
                {
                    if (ennMatches.Contains(annMatch))
                    {
                        correctCount++;
                    }
                }

                if ((i++ % 100) == 0)
                {
                    Console.WriteLine($"{correctCount} - {results} = {correctCount / (float)(results):P} matches");
                }
            }
        }

        Console.WriteLine($"{correctCount} - {queries * resultsCount} {correctCount / (float)(queries * resultsCount):P} matches");
    }
}

async Task ImportData(string path)
{
    var options = StorageEnvironmentOptions.ForPathForTests(path);
    Console.WriteLine(options.BasePath.FullPath);
    using var env = new StorageEnvironment(options);

    using (var txw = env.WriteTransaction())
    {
        Hnsw.Create(txw.LowLevelTransaction, "wiki", 768 * 4, 12, 40, VectorEmbeddingType.Single);
        txw.Commit();
    }

    foreach (var file in Files)
    {
        var fullPath = Path.Combine(Path.GetTempPath(), "Vector.Benchmark", file);
        if (File.Exists(fullPath) is false)
        {
            await DownloadFile(fullPath);
        }

        foreach (var (ids, vectors) in YieldVectors(fullPath))
        {
            var batch = Stopwatch.StartNew();
            using (var txw = env.WriteTransaction())
            {
                using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, "wiki"))
                {
                    registration.Random = new Random(454);
                    for (int i = 0; i < ids.Length; i++)
                    {
                        var vector = new Memory<float>(vectors, i * 768, 768);
                        registration.Register(ids[i] * 100, MemoryMarshal.Cast<float, byte>(vector.Span));
                    }
                    registration.Commit();
                }
                txw.Commit();
            }
            Console.WriteLine($" * {ids.Length:N0} - {batch.Elapsed}");
        }
    }
}


static IEnumerable<(int[], float[])> YieldVectors(string filePath)
{
    var file = ParquetReader.CreateAsync(filePath).Result;
    var schema = file.Schema;
    for (int i = 0; i < file.RowGroupCount; i++)
    {
        var reader = file.OpenRowGroupReader(i);
        var wikiId = reader.ReadColumnAsync(schema.DataFields[4]).Result;
        var vectors = reader.ReadColumnAsync(schema.DataFields[8]).Result;
        var wikiIds = (int[])wikiId.DefinedData;
        var vectorsArr = (float[])vectors.DefinedData;
        yield return (wikiIds, vectorsArr);
    }
}

static async Task DownloadFile(string fullPath)
{
    const string url = "https://huggingface.co/datasets/Cohere/wikipedia-22-12-simple-embeddings/resolve/main/data/";

    Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);
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
