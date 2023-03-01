using BenchmarkDotNet.Running;
using BenchmarkDotNet.Attributes;
using Tests.Infrastructure;
using Xunit.Abstractions;
using System.Diagnostics;
using System.Text;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Validators;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Server.Rachis;
using Raven.Tests.Core.Utils.Entities;
using Raven.Server;
using Org.BouncyCastle.Asn1.X509;

namespace ServerStoreTxMerger.Benchmark;

public class ServerStoreTxMergerBenchRealClusterTests
{
    private static readonly List<(int Count, int Size)> _numAndSizeOfDocs = new List<(int, int)> { (30000, 1_024) };
    public static Dictionary<string, Doc[]> DocsArrays = new Dictionary<string, Doc[]>();
    public static List<string> DocsArraysKeys { get; set; } = new List<string>();

    [ParamsSource(nameof(DocsArraysKeys))]
    public string DocsGroup { get; set; }

    [Params(1, 3)]
    public int NumOfNodes { get; set; }

    static ServerStoreTxMergerBenchRealClusterTests()
    {
        CreateDocs();
    }

    private static void CreateDocs()
    {
        var strRan = new StringRandom();

        foreach (var pair in _numAndSizeOfDocs)
        {
            var numOfDocs = pair.Count;
            var docSizeInBytes = pair.Size;

            var docs = new Doc[numOfDocs];

            for (int i = 0; i < numOfDocs; i++)
            {
                var randomData = strRan.GetRandomData(docSizeInBytes);
                docs[i] = new Doc
                {
                    Id = $"Docs/{i}",
                    Data = randomData
                };
            }

            string sizeStr = docSizeInBytes + " Bytes";
            if (docSizeInBytes / 1024 * 1024 > 0)
            {
                sizeStr = docSizeInBytes / (1024 * 1024) + " MB";
            }
            else if (docSizeInBytes / 1024 > 0)
            {
                sizeStr = docSizeInBytes / 1024 + " KB";
            }
            var key = $"{numOfDocs} of {sizeStr}";
            DocsArraysKeys.Add(key);
            DocsArrays[key] = docs;
        }
    }


    private MyOutputHelper? _testOutputHelper = new MyOutputHelper();
    private ActualClusterTests? _tests;


    [IterationSetup(Targets = new[] { nameof(ClusterTest) })]
    public void BeforeTest()
    {
        _tests = new ActualClusterTests(_testOutputHelper);
        _tests.Initialize(NumOfNodes).GetAwaiter().GetResult();
    }

    [Benchmark]
    public async Task ClusterTest()
    {
        await _tests.Test(DocsArrays[DocsGroup]);
    }

    [IterationCleanup(Targets = new[] { nameof(ClusterTest) })]
    public void AfterTest()
    {
        _tests.Dispose();
    }

}


public class ActualClusterTests : ClusterTestBase
{
    private List<RavenServer> _nodes;
    private RavenServer _leader;
    private DocumentStore _store;

    public ActualClusterTests(ITestOutputHelper output) : base(output)
    {
    }

    public async Task Initialize(int nodesCount)
    {
        (_nodes, _leader) = await CreateRaftCluster(numberOfNodes: nodesCount, watcherCluster: true, shouldRunInMemory: false);
        _store = GetDocumentStore(new Options() { Server = _leader, ReplicationFactor = nodesCount });
    }

    public async Task Test(Doc[] docs)
    {
        var tasks = new HashSet<Task>();

        for (int index = 0; index < docs.Length; index++)
        {
            var t = StoreClusterWide(_store, docs[index], index);

            tasks.Add(t);

            if (tasks.Count < 1500)
                continue;

            var finished = await Task.WhenAny(tasks);
            tasks.Remove(finished);
        }

        await Task.WhenAll(tasks);
    }

    private static async Task StoreClusterWide(DocumentStore store, Doc doc, int index1)
    {
        using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
        {
            await session.StoreAsync(doc);
            await session.SaveChangesAsync();
        }
    }

    public override void Dispose()
    {
        _leader = null;
        _nodes = null;
        _store = null;
        base.Dispose();
    }
}
