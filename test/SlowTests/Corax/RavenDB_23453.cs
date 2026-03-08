using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Numerics.Tensors;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Corax.Utils;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Timings;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public class RavenDB_23453_Integration(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenMultiplatformTheory(RavenTestCategory.Querying | RavenTestCategory.Corax | RavenTestCategory.Vector, RavenArchitecture.AllX64)]
    [InlineData(true, true)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(false, false)]
    public async Task VectorAnd(bool disableScanning, bool isExact)
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        InsertDocumentsAndIndex(store, out var indexName);
        if (disableScanning)
            await DisableScanning(store, indexName);

        using var session = store.OpenAsyncSession();
        var result = await session.Query<Dto, Index>()
            .Where(x => x.Counter0 == 1 && x.Counter1 <= 3)
            .VectorSearch(f => f.WithField(s => s.Text), v => v.ByText("car"), numberOfCandidates: 2, isExact: isExact)
            .ToListAsync();

        WaitForUserToContinueTheTest(store);
        Assert.Equal(6, result.Count);
        Assert.Equal("bike", result[^1].Text);
    }

    private void InsertDocumentsAndIndex(DocumentStore store, out string indexName)
    {
        using (var bulkInsert = store.BulkInsert())
        {
            bulkInsert.Store(new Dto { Text = "car", Counter0 = 1, Counter1 = 3 });
            bulkInsert.Store(new Dto { Text = "car", Counter0 = 1, Counter1 = 3 });
            bulkInsert.Store(new Dto { Text = "car", Counter0 = 1, Counter1 = 3 });
            bulkInsert.Store(new Dto { Text = "car", Counter0 = 1, Counter1 = 3 });
            bulkInsert.Store(new Dto { Text = "car", Counter0 = 1, Counter1 = 3 });
            bulkInsert.Store(new Dto { Text = "cars", Counter0 = 2, Counter1 = 3 });
            bulkInsert.Store(new Dto { Text = "bike", Counter0 = 1, Counter1 = 2 });
        }

        var index = new Index();
        index.Execute(store);
        indexName = index.IndexName;
        Indexes.WaitForIndexing(store);
    }

    private async Task DisableScanning(DocumentStore store, string indexName)
    {
        var database = await Databases.GetDocumentDatabaseInstanceFor(store, store.Database);
        var index = database.IndexStore.GetIndex(indexName);
        index.ForTestingPurposesOnly().CoraxConfiguration = new CoraxTestingConfiguration()
        {
            DisableVectorSearchScanning = true,
        };
    }

    private class Index : AbstractIndexCreationTask<Dto>
    {
        public Index()
        {
            Map = dtos => from doc in dtos
                select new
                {
                    Text = CreateVector(doc.Text.ToString()),
                    Vector = CreateVector(doc.Vector),
                    TextFts = doc.Text,
                    doc.Counter0,
                    doc.Counter1,
                    doc.StartsWith,
                    doc.Constant,
                };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    [DebuggerDisplay("({Id}) V: {Vector} C0: {Counter0} C1: {Counter1}")]
    private class Dto
    {
        public string Id { get; set; }
        public string Text { get; set; }
        public float[] Vector { get; set; }
        public int Counter0 { get; set; }
        public int Counter1 { get; set; }
        public string StartsWith { get; set; }
        public string Constant { get; set; }
    }


    [RavenMultiplatformTheory(RavenTestCategory.Vector | RavenTestCategory.Querying | RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    [InlineDataWithRandomSeed(true, true)]
    [InlineDataWithRandomSeed(true, false)]
    [InlineDataWithRandomSeed(false, true)]
    [InlineDataWithRandomSeed(false, false)]
    [InlineDataWithRandomSeed(true, true)]
    [InlineDataWithRandomSeed(true, false)]
    [InlineDataWithRandomSeed(false, true)]
    [InlineDataWithRandomSeed(false, false)]
    [InlineDataWithRandomSeed(true, true)]
    [InlineDataWithRandomSeed(true, false)]
    [InlineDataWithRandomSeed(false, true)]
    [InlineDataWithRandomSeed(false, false)]
    [InlineDataWithRandomSeed(true, true)]
    [InlineDataWithRandomSeed(true, false)]
    [InlineDataWithRandomSeed(false, true)]
    [InlineDataWithRandomSeed(false, false)]
    [InlineData(true, true, 1035466315)]
    public void TestAndGenerateRandomQueryVectors(bool disableScanning, bool isExact, int seed)
    {
        var random = new Random(seed);
        using var store = GetDocumentStore();
        var db = GenerateData(store, random, out var indexName);
        if (disableScanning)
            AsyncHelpers.RunSync(() => DisableScanning(store!, indexName));

        var dbAsSpan = CollectionsMarshal.AsSpan(db);
        using var session = store.OpenSession();
        var expectedDocumentsCount = random.Next(4, 32);
        random.Shuffle(dbAsSpan);
        var toFind = dbAsSpan[..expectedDocumentsCount].ToArray();

        var expectedMethods = random.Next((int)MethodSearch.Search, (int)MethodSearch.All + 1);
        var methodToInvoke = Enumerable.Range(1, 9).Where(x => ((1 << x) & expectedMethods) != 0).Select(p => (MethodSearch)(1 << p)).ToList();
        random.Shuffle(CollectionsMarshal.AsSpan(methodToInvoke));
        var r = session.Advanced.DocumentQuery<Dto, Index>();
        bool isFirst = true;

        methodToInvoke.Add(MethodSearch.VectorSearch);
        random.Shuffle(CollectionsMarshal.AsSpan(methodToInvoke)[1..]);
        var pos = random.Next(0, 360);
        var x = MathF.Cos(pos * MathF.PI / 180);
        var y = MathF.Sin(pos * MathF.PI / 180);
        float[] vectorToSearch = [x, y];
        IEnumerable<Dto> actualMatches = db;
        foreach (var queryType in methodToInvoke)
        {
            if (isFirst == false)
                r.AndAlso();
            isFirst = false;
            switch (queryType)
            {
                case MethodSearch.Search:
                case MethodSearch.In:
                {
                    var terms = toFind.Select(x => x.Text).Distinct().ToArray();
                    if (queryType == MethodSearch.Search)
                        r.Search("TextFts", string.Join(" ", terms));
                    else
                        r.WhereIn("TextFts", terms);

                    actualMatches = actualMatches.Where(p => terms.Contains(p.Text));
                    break;
                }
                case MethodSearch.StartsWith:
                {
                    var allStartsWith = toFind.Select(x => x.StartsWith).ToArray();
                    var len = allStartsWith.Min(x => x.Length);
                    string prefix = "_";
                    for (int i = 1; i < len; ++i)
                    {
                        var currentPrefix = allStartsWith[0].Substring(0, i);
                        if (allStartsWith.Count(p => p.StartsWith(currentPrefix)) == allStartsWith.Length)
                            prefix = currentPrefix;
                        else break;
                    }

                    actualMatches = actualMatches.Where(p => p.StartsWith.StartsWith(prefix));
                    r.WhereStartsWith(p => p.StartsWith, prefix);
                    break;
                }
                case MethodSearch.EndsWith:
                {
                    var allEndsWith = toFind.Select(x => string.Join("", x.StartsWith.Reverse().ToArray())).ToArray();
                    var len = allEndsWith.Min(x => x.Length);
                    string suffix = "_";
                    for (int i = 1; i < len; ++i)
                    {
                        var currentSuffix = allEndsWith[0].Substring(0, i);
                        if (allEndsWith.Count(p => p.StartsWith(currentSuffix)) == allEndsWith.Length)
                            suffix = currentSuffix;
                        else break;
                    }


                    suffix = string.Join("", suffix.Reverse().ToArray());
                    r.WhereEndsWith(p => p.StartsWith, suffix);
                    actualMatches = actualMatches.Where(p => p.StartsWith.EndsWith(suffix));
                    break;
                }
                case MethodSearch.Exists:
                {
                    r.WhereExists(p => p.Id);
                    break;
                }
                case MethodSearch.LessThan:
                {
                    var max = toFind.Max(x => x.Counter0);
                    r.WhereLessThan(p => p.Counter0, max + 1);
                    actualMatches = actualMatches.Where(p => p.Counter0 < max + 1);
                    break;
                }
                case MethodSearch.GreaterThan:
                {
                    var min = toFind.Min(x => x.Counter0);
                    r.WhereGreaterThan(p => p.Counter0, min - 1);
                    actualMatches = actualMatches.Where(p => p.Counter0 > min - 1);
                    break;
                }
                case MethodSearch.Equals:
                    r.WhereEquals(p => p.Constant, Constant);
                    break;
                case MethodSearch.Between:
                {
                    var max = toFind.Max(x => x.Counter0);
                    var min = toFind.Min(x => x.Counter0);
                    r.WhereBetween(p => p.Counter0, min - 1, max + 1);
                    actualMatches = actualMatches.Where(p => p.Counter0 >= min - 1 && p.Counter0 <= max + 1);
                    break;
                }
                case MethodSearch.VectorSearch:
                {
                    r.VectorSearch(x => x.WithField(p => p.Vector), f => f.ByEmbedding(vectorToSearch), numberOfCandidates: 400, isExact: isExact, minimumSimilarity: 0.75f);
                    break;
                }
            }
        }

        var serverMaterialized = r.Timings(out QueryTimings timings).ToList();
        var localVectorSearch = (from doc in actualMatches
            let similarity = 1 - TensorPrimitives.CosineSimilarity(doc.Vector, vectorToSearch.AsSpan())
            where similarity <= (2f * (1.0f - 0.75f) + 0.001f) // 0.75 <- minimum match, 0.001 is eps
            orderby similarity ascending
            select doc.Id).ToList();
        
        foreach (var docId in serverMaterialized.Select(p => p.Id))
            Assert.Contains(docId, localVectorSearch);
    }


    private const string Constant = "constant";

    private enum MethodSearch : int
    {
        Search = 1,
        StartsWith = 1 << 1,
        EndsWith = 1 << 2,
        Exists = 1 << 3,
        In = 1 << 4,
        Between = 1 << 5,
        LessThan = 1 << 6,
        GreaterThan = 1 << 7,
        Equals = 1 << 8,
        VectorSearch = 1 << 9,
        All = Search | StartsWith | EndsWith | Exists | In | Between | LessThan | GreaterThan | Equals,
    }

    private List<Dto> GenerateData(DocumentStore store, Random random, out string indexName)
    {
        List<Dto> db = new();

        using (var bulkInsert = store.BulkInsert())
        {
            HashSet<string> prefixes = new();
            while (prefixes.Count < 360 / 32f)
            {
                prefixes.Add(RandomString(random, 4));
            }

            string[] terms =
            [
                "dog", "cat", "car", "bike", "lake", "yellow", "programming", "computer", "tea", "coffee", "keyboard", "guitar",
                "list", "test", "program", "sea", "ocean", "laptop", "console", "language", "flat", "house", "ship", "airplane", "airport", "island", "tshirt", "coat"
            ];


            var prefixesList = prefixes.ToList();

            for (int i = 0; i < 360; ++i)
            {
                var x = MathF.Cos(i * MathF.PI / 180);
                var y = MathF.Sin(i * MathF.PI / 180);
                var d = new Dto()
                {
                    Text = terms[random.Next() % terms.Length],
                    Vector = new float[] { x, y },
                    StartsWith = prefixesList[random.Next() % prefixesList.Count],
                    Constant = Constant,
                    Counter0 = random.Next(1, 100),
                };
                bulkInsert.Store(d);
                db.Add(d);
            }
        }

        var index = new Index();
        index.Execute(store);
        indexName = index.IndexName;
        Indexes.WaitForIndexing(store);

        return db;
    }

    private static string RandomString(Random random, int length)
    {
        const string chars = "abcdefghijklmnopqrstuvwxyz";
        var str = new char[length + 2];
        str[0] = '_';
        str[^1] = '_';
        for (int i = 0; i < length; i++)
        {
            str[i + 1] = chars[random.Next(chars.Length)];
        }

        return new string(str);
    }
}
