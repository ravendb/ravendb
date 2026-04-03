using System.Diagnostics;
using System.Runtime.InteropServices;
using Parquet;
using Voron;
using Voron.Data.Graphs;

string[] files = ["wikipedia-22-12-simple-embeddings_train.parquet",];

var dbPath = Path.GetFullPath("vectors");
if (Directory.Exists(dbPath))
{
    Directory.Delete(dbPath, true);
}

await ImportData(dbPath);

var sp = Stopwatch.StartNew();
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
    foreach (var file in files)
    {
        using var txr = env.ReadTransaction();
        using var _ = Slice.From(txr.Allocator, "wiki", out var wikiTreeName);
        var fullPath = Path.Combine(Path.GetTempPath(), "Vector.Benchmark", file);

        foreach (var (ids, vectors) in YieldVectors(fullPath))
        {
            for (int i = 0; i < ids.Length; i++)
            {
                var vector = new Span<float>(vectors, i * 768, 768);
                
                using var enn = Hnsw.ExactNearest(txr.LowLevelTransaction, wikiTreeName, 10, MemoryMarshal.AsBytes(vector).ToArray(), 0f, false);
                using var ann = Hnsw.ApproximateNearest(txr.LowLevelTransaction, wikiTreeName, 64, MemoryMarshal.AsBytes(vector).ToArray(), 0f, false);

                var aRead = ann.Fill(annMatches, annDistances, null);
                var eRead = enn.Fill(ennMatches, ennDistances, null);
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

    TimeSpan import = TimeSpan.Zero;
    
    using (var txw = env.WriteTransaction())
    {
        Hnsw.Create(txw.LowLevelTransaction, "wiki", 768 * 4, 12, 40, VectorEmbeddingType.Single);
        txw.Commit();
    }

    foreach (var file in files)
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
                    registration.Commit(CancellationToken.None);
                }
                txw.Commit();
            }

            var elapsed = batch.Elapsed;
            Console.WriteLine($" * {ids.Length:N0} - {elapsed}");
            import += elapsed;
        }
    }
    
    Console.WriteLine($"Import took: {import}");
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
    const string url = "https://hub.oxen.ai/api/repos/Cohere/wikipedia-22-12-simple-embeddings/file/main/";

    Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);
    using (HttpClient client = new())
    {
        client.Timeout = TimeSpan.FromHours(1); // Set timeout to a reasonable value for large files
        
        string path = url + Path.GetFileName(fullPath);

        using (var response = await client.GetAsync(path, HttpCompletionOption.ResponseHeadersRead))
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
