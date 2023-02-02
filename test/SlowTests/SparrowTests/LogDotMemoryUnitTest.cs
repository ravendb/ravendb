using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using FastTests;
using JetBrains.dotMemoryUnit;
using Raven.Server.Utils.MicrosoftLogging;
using Sparrow;
using Sparrow.Logging;
using Xunit;
using Xunit.Abstractions;
using Memory = JetBrains.dotMemoryUnit.Memory;
using Size = Sparrow.Size;

namespace SlowTests.SparrowTests;

public class LogDotMemoryUnitTest : RavenTestBase
{
    public LogDotMemoryUnitTest(ITestOutputHelper output) : base(output)
    {
        DotMemoryUnitTestOutput.SetOutputMethod(s => { });
    }


    class MyStream : Stream
    {
        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            return count;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {

        }

        public override bool CanRead { get; }
        public override bool CanSeek { get; }
        public override bool CanWrite { get; } = true;
        public override long Length { get; }
        public override long Position { get; set; }
    }


    [Fact]
    [DotMemoryUnit(
        CollectAllocations = true,
        SavingStrategy = SavingStrategy.OnAnyFail,
        Directory = @"C:\work\raven\RavenDB-19726\Snapshots",
        WorkspaceNumberLimit = 10)]
    public void MainLogs()
    {
        var round = 100000;

        var loggingSource = new LoggingSource(LogMode.None, Path.Combine(@"C:\work\raven\RavenDB-19726\temp", Guid.NewGuid().ToString("N")), "", TimeSpan.MaxValue,
            long.MaxValue);
        var myStream = new MyStream();
        loggingSource.AttachPipeSink(myStream);
        var logger = loggingSource.GetLogger("haludisource", "haludilogger");

        var memoryCheckPoint1 = dotMemory.Check();
        {
            int i1 = 1203, i2 = 1204, i3 = 1205, i4 = 1206, i5 = 1207;
            for (int j = 0; j < round; j++)
                logger.Info($"{i1}aaaaaaaaaaaaaaaa{i2}aaaaaaaaaaaaaaaaaa{i3}aaaaaaaaaaaaaaaaaaa{i4}aaaaaaaaaa{i5}aaaaaaaaaaaa");
        }
        var memoryCheckPoint2 = dotMemory.Check(m => CheckAndPrintReport(m, memoryCheckPoint1));
        {
            int i1 = 1203, i2 = 1204, i3 = 1205, i4 = 1206, i5 = 1207;
            for (int j = 0; j < round; j++)
                logger.InfoDirectlyToStream((s, args) => s.InterpolateDirectly($"{args.i1}aaaaaaaaaaaaaaaa{args.i2}aaaaaaaaaaaaaaaaaa{args.i3}aaaaaaaaaaaaaaaaaaa{args.i4}aaaaaaaaaa{args.i5}aaaaaaaaaaaa"),
                    (i1, i2, i3, i4, i5));
        }
        var memoryCheckPoint3 = dotMemory.Check(m => CheckAndPrintReport(m, memoryCheckPoint2));
        {
            int i1 = 1203, i2 = 1204, i3 = 1205, i4 = 1206, i5 = 1207;
            for (int j = 0; j < round; j++)
                logger.UseArrayPool($"{i1}aaaaaaaaaaaaaaaa{i2}aaaaaaaaaaaaaaaaaa{i3}aaaaaaaaaaaaaaaaaaa{i4}aaaaaaaaaa{i5}aaaaaaaaaaaa");
        }
        var memoryCheckPoint4 = dotMemory.Check(m => CheckAndPrintReport(m, memoryCheckPoint3));
        {
            int i1 = 1203, i2 = 1204, i3 = 1205, i4 = 1206, i5 = 1207;
            Span<char> initBuffer = stackalloc char[128];
            for (int j = 0; j < round; j++)
                logger.UseArrayPool(initBuffer, $"{i1}aaaaaaaaaaaaaaaa{i2}aaaaaaaaaaaaaaaaaa{i3}aaaaaaaaaaaaaaaaaaa{i4}aaaaaaaaaa{i5}aaaaaaaaaaaa");
        }
        
        dotMemory.Check(m => CheckAndPrintReport(m, memoryCheckPoint4));
        Assert.True(false);
    }

    private static void CheckAndPrintReport(Memory memory, MemoryCheckPoint previous, [CallerArgumentExpression("previous")] string strExpression = "")
    {
        Traffic traffic = memory.GetTrafficFrom(previous);
        var path = @"C:\work\raven\RavenDB-19726\Memory";

        var builder = new StringBuilder(
            $"Total SizeInBytes:{new Size(traffic.AllocatedMemory.SizeInBytes, SizeUnit.Bytes)} ObjectsCount:{traffic.AllocatedMemory.ObjectsCount}");
        builder.AppendLine();
        builder.AppendLine();
        var orderBy = traffic.GroupByType()
            .OrderByDescending(x => x.AllocatedMemoryInfo.SizeInBytes)
            .Select(x => $"Type {x.Type}: " +
                         $"Count:{x.AllocatedMemoryInfo.ObjectsCount} " +
                         $"Size{new Size(x.AllocatedMemoryInfo.SizeInBytes, SizeUnit.Bytes)}");
        foreach (var line in orderBy)
        {
            builder.AppendLine(line);
        }

        File.WriteAllText(Path.Combine(path, $"{strExpression}.txt"), builder.ToString());
    }
}

