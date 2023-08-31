using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Tests.Infrastructure;
using Xunit.Abstractions;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using System.Threading;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Exceptions.Database;
using Raven.Server;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using System.Diagnostics;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.Handlers.Batches.Commands;
using Raven.Server.Documents.Handlers.Batches;

namespace ServerStoreTxMerger.Benchmark;

public class ServerStoreTxMergerBenchRealClusterTests
{
    private static readonly List<(int Count, int Size)> _numAndSizeOfDocs = new List<(int, int)> { (30_00, 1_024) };
    public static Dictionary<string, DynamicJsonValue[]> DocsArrays = new Dictionary<string, DynamicJsonValue[]>();
    public static List<string> DocsArraysKeys { get; set; } = new List<string>();

    [ParamsSource(nameof(DocsArraysKeys))]
    public string DocsGroup { get; set; }

    [Params(1, 3)]
    public int NumOfNodes { get; set; }

    static ServerStoreTxMergerBenchRealClusterTests()
    {
        CreateCmpxg();
    }

    private static void CreateDocs()
    {
        var strRan = new StringRandom();

        foreach (var pair in _numAndSizeOfDocs)
        {
            var numOfDocs = pair.Count;
            var docSizeInBytes = pair.Size;

            var docs = new DynamicJsonValue[numOfDocs];

            for (int i = 0; i < numOfDocs; i++)
            {
                var randomData = strRan.GetRandomData(docSizeInBytes);
                docs[i] = new DynamicJsonValue()
                {
                    ["Id"] = $"Docs/{i}",
                    ["Data"] = randomData
                };
            }

            string sizeStr = docSizeInBytes + " Bytes";
            if (docSizeInBytes / (1024 * 1024) > 0)
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


    private static void CreateCmpxg()
    {
        var strRan = new StringRandom();

        foreach (var pair in _numAndSizeOfDocs)
        {
            var numOfDocs = pair.Count;
            var docSizeInBytes = pair.Size;

            var docs = new DynamicJsonValue[numOfDocs];

            for (int i = 0; i < numOfDocs; i++)
            {
                var randomData = strRan.GetRandomData(docSizeInBytes);
                docs[i] = new DynamicJsonValue()
                {
                    ["Object"] = randomData
                };
            }

            string sizeStr = docSizeInBytes + " Bytes";
            if (docSizeInBytes / (1024 * 1024) > 0)
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


    private static MyOutputHelper _testOutputHelper = new MyOutputHelper();
    private ActualClusterTests _tests;


    [IterationSetup(Targets = new[] { nameof(ClusterTest) })]
    public void BeforeTest()
    {
        _tests = new ActualClusterTests(_testOutputHelper);
        _tests.Initialize(NumOfNodes).GetAwaiter().GetResult();
    }

    [Benchmark]
    public async Task ClusterTest()
    {
        if (_tests == null)
        {
            throw new InvalidOperationException("\'_tests\' cannot be null");
        }
        await _tests.Test(DocsArrays[DocsGroup]);
    }

    [IterationCleanup(Targets = new[] { nameof(ClusterTest) })]
    public void AfterTest()
    {
        _tests?.Dispose();
    }

}


public class ActualClusterTests : ClusterTestBase
{
    private DocumentStore _store;
    private RavenServer _leader;
    private long _index = 0;

    public ActualClusterTests(ITestOutputHelper output) : base(output)
    {
    }

    public async Task Initialize(int nodesCount)
    {
        var (nodes, leader) = await CreateRaftCluster(numberOfNodes: nodesCount, watcherCluster: true, shouldRunInMemory: false);
        _store = GetDocumentStore(new Options() { Server = leader, ReplicationFactor = nodesCount });
        _leader = leader;

        DatabaseRecordWithEtag record = null;
        while (record == null || record.Topology.Count != 1)
        {
            record = _store.Maintenance.Server.Send(new GetDatabaseRecordOperation(_store.Database));
            Thread.Sleep(100);
        }
    }

    public async Task Test(DynamicJsonValue[] docs)
    {
        if (_store == null)
        {
            throw new InvalidOperationException("store cannot be null");
        }

        if (_leader == null)
        {
            throw new InvalidOperationException("leader cannot be null");
        }

        var sw = Stopwatch.StartNew();
        var tasks = new HashSet<Task>();

        for (int index = 0; index < docs.Length; index++)
        {
            var t = StoreCompareExchange(_store, docs[index], index);

            tasks.Add(t);

            if (tasks.Count < 1500)
                continue;

            var finished = await Task.WhenAny(tasks);
            tasks.Remove(finished);
        }


        await Task.WhenAll(tasks);

        // var lastDoc = docs[docs.Length - 1];
        // var id = (string)lastDoc["Id"];
        // Doc d = null;
        // while (d == null)
        // {
        //     using (var session = _store.OpenSession())
        //     {
        //         d = session.Load<Doc>(id);
        //     }
        //
        //     await Task.Delay(200);
        // }
    }

    private async Task StoreClusterWide(DocumentStore store, DynamicJsonValue doc, int index1)
    {

        var db = await _leader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(_store.Database);
        var id = (string)doc["Id"];

        using var context = JsonOperationContext.ShortTermSingleUse();

        var blittable = context.ReadObject(doc, id);

        var options = new ClusterTransactionCommand.ClusterTransactionOptions(Guid.NewGuid().ToString(), disableAtomicDocumentWrites: false, clusterMinVersion: 60_000)
        {
            WaitForIndexesTimeout = null,
            WaitForIndexThrow = true,
            SpecifiedIndexesQueryString = null
        };

        var commands = new[]
        {
            new BatchRequestParser.CommandData
            {
                Id = id,
                Document = blittable,
                AttachmentStream = new MergedBatchCommand.AttachmentStream { Hash = null, Stream = null },
                Type = CommandType.PUT,
                ForceRevisionCreationStrategy = ForceRevisionStrategy.None
            },
            // new BatchRequestParser.CommandData
            // {
            //     Type = CommandType.None,
            //     ForceRevisionCreationStrategy = ForceRevisionStrategy.None
            // },
            // new BatchRequestParser.CommandData
            // {
            //     Type = CommandType.None,
            //     ForceRevisionCreationStrategy = ForceRevisionStrategy.None
            // },
            // new BatchRequestParser.CommandData
            // {
            //     Type = CommandType.None,
            //     ForceRevisionCreationStrategy = ForceRevisionStrategy.None
            // },
            // new BatchRequestParser.CommandData
            // {
            //     Type = CommandType.None,
            //     ForceRevisionCreationStrategy = ForceRevisionStrategy.None
            // },
            // new BatchRequestParser.CommandData
            // {
            //     Type = CommandType.None,
            //     ForceRevisionCreationStrategy = ForceRevisionStrategy.None
            // },
            // new BatchRequestParser.CommandData
            // {
            //     Type = CommandType.None,
            //     ForceRevisionCreationStrategy = ForceRevisionStrategy.None
            // },
            // new BatchRequestParser.CommandData
            // {
            //     Type = CommandType.None,
            //     ForceRevisionCreationStrategy = ForceRevisionStrategy.None
            // },
        };
        ArraySegment<BatchRequestParser.CommandData> parsedCommands = commands;

        var raftRequestId = Interlocked.Increment(ref _index).ToString();
        var topology = _leader.ServerStore.LoadDatabaseTopology(db.Name);

        if (topology.Promotables.Contains(_leader.ServerStore.NodeTag))
            throw new DatabaseNotRelevantException("Cluster transaction can't be handled by a promotable node.");

        var clusterTransactionCommand = new ClusterTransactionCommand(db.Name, db.IdentityPartsSeparator, topology, parsedCommands, options, raftRequestId);
        var result = await _leader.ServerStore.SendToLeaderAsync(clusterTransactionCommand);

        if (result.Result is List<ClusterTransactionCommand.ClusterTransactionErrorInfo> errors)
        {
            throw new InvalidOperationException("Command ended with errors");
        }

        await _leader.ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, result.Index);
    }

    private async Task StoreCompareExchange(DocumentStore store, DynamicJsonValue doc, int index1)
    {

        var db = await _leader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(_store.Database);
        var id = $"Cmp{index1}";

        using var context = JsonOperationContext.ShortTermSingleUse();

        var blittable = context.ReadObject(doc, id);

        var options = new ClusterTransactionCommand.ClusterTransactionOptions(Guid.NewGuid().ToString(), disableAtomicDocumentWrites: false, clusterMinVersion: 60_000)
        {
            WaitForIndexesTimeout = null,
            WaitForIndexThrow = true,
            SpecifiedIndexesQueryString = null
        };

        var commands = new[]
        {
            new BatchRequestParser.CommandData
            {
                Id = id,
                Document = blittable,
                AttachmentStream = new MergedBatchCommand.AttachmentStream { Hash = null, Stream = null },
                Type = CommandType.CompareExchangePUT,
                ForceRevisionCreationStrategy = ForceRevisionStrategy.None
            },
        };
        ArraySegment<BatchRequestParser.CommandData> parsedCommands = commands;

        var raftRequestId = Interlocked.Increment(ref _index).ToString();
        var topology = _leader.ServerStore.LoadDatabaseTopology(db.Name);

        if (topology.Promotables.Contains(_leader.ServerStore.NodeTag))
            throw new DatabaseNotRelevantException("Cluster transaction can't be handled by a promotable node.");

        var clusterTransactionCommand = new ClusterTransactionCommand(db.Name, db.IdentityPartsSeparator, topology, parsedCommands, options, raftRequestId);
        var result = await _leader.ServerStore.SendToLeaderAsync(clusterTransactionCommand);

        if (result.Result is List<ClusterTransactionCommand.ClusterTransactionErrorInfo> errors)
        {
            throw new InvalidOperationException("Command ended with errors");
        }

        await _leader.ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, result.Index);
    }

    public override void Dispose()
    {
        var dbName = _store.Database;
        var results = _store
            .Maintenance
            .Server
            .Send(new DeleteDatabasesOperation(new DeleteDatabasesOperation.Parameters()
            {
                DatabaseNames = new[] { dbName },
                HardDelete = true
            }));

        DatabaseRecordWithEtag record = null;
        do
        {
            Thread.Sleep(100);
            record = _store.Maintenance.Server.Send(new GetDatabaseRecordOperation(dbName));
        } while (record != null && record.Topology.Count > 0);

        _store.Dispose();
        _store = null;
        _leader = null;
        base.Dispose();
    }
}
