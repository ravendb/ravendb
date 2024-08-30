using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Tests.Infrastructure;
using Xunit.Abstractions;
using Raven.Server.Rachis;

namespace ServerStoreTxMerger.Benchmark;

public class ServerStoreTxMergerBenchTests
{
    private static readonly List<(int Count, int Size)> _numAndSizeOfCmds = new List<(int, int)> { (30000, 1_024) };
    public static Dictionary<string, RachisConsensusTestBase.TestCommandWithLargeData[]> CmdsArrays = new Dictionary<string, RachisConsensusTestBase.TestCommandWithLargeData[]>();
    public static List<string> CmdsArraysKeys { get; set; } = new List<string>();

    [ParamsSource(nameof(CmdsArraysKeys))]
    public string CommandsGroup { get; set; }

    [Params(1, 3)]
    public int NumOfNodes { get; set; }

    static ServerStoreTxMergerBenchTests()
    {
        CreateCmds();
    }

    private static void CreateCmds()
    {
        var strRan = new StringRandom();

        foreach (var pair in _numAndSizeOfCmds)
        {
            var numOfCmds = pair.Count;
            var cmdSizeInBytes = pair.Size;

            var cmds = new RachisConsensusTestBase.TestCommandWithLargeData[numOfCmds];

            for (int i = 0; i < numOfCmds; i++)
            {
                var randomData = strRan.GetRandomData(cmdSizeInBytes);
                cmds[i] = new RachisConsensusTestBase.TestCommandWithLargeData
                {
                    Name = $"test{i}",
                    RandomData = randomData,
                    Timeout = TimeSpan.MaxValue
                };
            }

            string sizeStr = cmdSizeInBytes + " Bytes";
            if (cmdSizeInBytes / (1024 * 1024) > 0)
            {
                sizeStr = cmdSizeInBytes / (1024 * 1024) + " MB";
            }
            else if (cmdSizeInBytes / 1024 > 0)
            {
                sizeStr = cmdSizeInBytes / 1024 + " KB";
            }
            var key = $"{numOfCmds} of {sizeStr}";
            CmdsArraysKeys.Add(key);
            CmdsArrays[key] = cmds;
        }
    }


    private static MyOutputHelper _testOutputHelper = new MyOutputHelper();
    private ActualTests _tests;


    [IterationSetup(Targets = new[] { nameof(Test) })]
    public void BeforeTest()
    {
        _tests = new ActualTests(_testOutputHelper);
        _tests.Initialize(NumOfNodes).GetAwaiter().GetResult();
    }

    [Benchmark]
    public async Task Test()
    {
        if (_tests == null)
        {
            throw new InvalidOperationException("\'_tests\' cannot be null");
        }
        await _tests.Test(CmdsArrays[CommandsGroup]);
    }

    [IterationCleanup(Targets = new[] { nameof(Test) })]
    public void AfterTest()
    {
        _tests?.Dispose();
    }

}

public class ActualTests : RachisConsensusTestBase
{
    private RachisConsensus<CountingStateMachine> _leader;

    public ActualTests(ITestOutputHelper output) : base(output)
    {
    }

    public async Task Initialize(int nodesCount)
    {
        _leader = await CreateNetworkAndGetLeader(nodeCount: nodesCount, watcherCluster: true, shouldRunInMemory: false);
    }

    public async Task Test(TestCommandWithLargeData[] cmds)
    {
        if (_leader == null)
        {
            throw new InvalidOperationException("leader cannot be null");
        }
        var tasks = new HashSet<Task>();
        foreach (var cmd in cmds)
        {
            var t = _leader.SendToLeaderAsync(cmd);
            tasks.Add(t);

            if (tasks.Count < 1500)
                continue;

            var finished = await Task.WhenAny(tasks);
            tasks.Remove(finished);
        }

        await Task.WhenAll(tasks);
    }

    public override void Dispose()
    {
        _leader = null;
        base.Dispose();
    }
}
