using BenchmarkDotNet.Running;
using BenchmarkDotNet.Attributes;
using Tests.Infrastructure;
using Xunit.Abstractions;
using System.Diagnostics;
using System.Text;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Validators;
using Raven.Server.Rachis;

namespace ServerStoreTxMerger.Benchmark;

public class ServerStoreTxMergerBench
{
    private static readonly List<(int Count,int Size)> _numAndSizeOfCmds = new List<(int, int)> { (3000, 1_000_000), (100, 5_000_000) };
    public static Dictionary<string, RachisConsensusTestBase.TestCommand[]> CmdsArrays = new Dictionary<string, RachisConsensusTestBase.TestCommand[]>();
    public static List<string> CmdsArraysKeys { get; set; } = new List<string>();

    [ParamsSource(nameof(CmdsArraysKeys))]
    public string CmdsArrayKey { get; set; }

    static ServerStoreTxMergerBench()
    {
        CreateCmds();
    }

    public static async Task Main(string[] args)
    {
        var config = new ManualConfig()
            .WithOptions(ConfigOptions.DisableOptimizationsValidator)
            .AddValidator(JitOptimizationsValidator.DontFailOnError)
            .AddLogger(ConsoleLogger.Default)
            .AddColumnProvider(DefaultColumnProviders.Instance);
        
        var summary = BenchmarkRunner.Run<ServerStoreTxMergerBench>(config);

        // Manual testing
        // var s = new ServerStoreTxMergerBench();
        // s.CmdsArrayKey = CmdsArraysKeys.First();
        // for (int i = 0; i < 10; i++)
        // {
        //     Console.WriteLine($"test {i}");
        //     s.BeforeTest();
        //     var sw = Stopwatch.StartNew();
        //     await s.Test();
        //     var time = sw.Elapsed;
        //     s.AfterTest();
        //     Console.WriteLine($"{time.Hours}:{time.Minutes}:{time.Seconds}");
        // }
    }

    private static string GetRandomData(int size)
    {
        var sb = new StringBuilder();
        var ran = new Random();
        int a = 'a' - 0;
        int z = 'z' - 0;
        for (int i = 0; i < size; i++)
        {
            var c = (char)(ran.Next(a, z + 1));
            sb.Append(c);
        }

        return sb.ToString();
    }

    private static void CreateCmds()
    {
        foreach(var pair in _numAndSizeOfCmds)
        {
            var numOfCmds = pair.Count;
            var cmdSizeInBytes = pair.Size;

            var cmds = new RachisConsensusTestBase.TestCommand[numOfCmds];

            for (int i = 0; i < numOfCmds; i++)
            {
                var randomData = GetRandomData(cmdSizeInBytes);
                cmds[i] = new RachisConsensusTestBase.TestCommand
                {
                    Name = $"test{i}",
                    Value = i,
                    RandomData = randomData,
                    Timeout = TimeSpan.MaxValue
                };
            }

            var key = $"{numOfCmds} commands in size {cmdSizeInBytes}";
            CmdsArraysKeys.Add(key);
            CmdsArrays[key] = cmds;
        }
    }


    private MyOutputHelper? _testOutputHelper = new MyOutputHelper();
    private ActualTests? _tests;
    
    
    [IterationSetup]
    public void BeforeTest()
    {
        _tests = new ActualTests(_testOutputHelper);
        _tests.Initialize().GetAwaiter().GetResult();
    }

    [Benchmark]
    public async Task Test()
    {
        await _tests.Test(CmdsArrays[CmdsArrayKey]);
    }

    [IterationCleanup]
    public void AfterTest()
    {
        _tests.Dispose();
    }

}


public class ActualTests : RachisConsensusTestBase
{
    private RachisConsensus<CountingStateMachine> _leader;

    public ActualTests(ITestOutputHelper output) : base(output)
    {
    }

    public async Task Initialize()
    {
        _leader = await CreateNetworkAndGetLeader(1);
    }

    public async Task Test(TestCommand[] cmds)
    {
        var tasks = new List<Task>();
        for (var i = 0; i < cmds.Length; i++)
        {
            var t = _leader.PutAsync(cmds[i]);
            tasks.Add(t);
        }

        await Task.WhenAll(tasks).WaitWithoutExceptionAsync(TimeSpan.MaxValue);
    }

    public override void Dispose()
    {
        _leader = null;
        base.Dispose();
    }
}

public class MyOutputHelper : ITestOutputHelper
{
    public void WriteLine(string message) => Console.WriteLine(message);

    public void WriteLine(string format, params object[] args) => Console.WriteLine(format, args);

    public void Dispose()
    {
    }
}
