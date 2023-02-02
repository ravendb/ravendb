using System;
using System.IO;
using BenchmarkDotNet.Attributes;
using Raven.Server.Utils.MicrosoftLogging;
using Sparrow.Logging;

namespace Micro.Benchmark.Benchmarks.Logs;

[MemoryDiagnoser]
public class BenchmarkLogWriteTests
{
    private  LoggingSource _loggingSource;
    private MyStream _myStream = new MyStream();
    private Logger _logger;
    
    [GlobalSetup]
    public void Setup()
    {
        _loggingSource = new LoggingSource(LogMode.None, Path.Combine(@"C:\work\raven\RavenDB-19726\temp", Guid.NewGuid().ToString("N")), "", TimeSpan.MaxValue, long.MaxValue);
        _myStream = new MyStream();
        _loggingSource.AttachPipeSink(_myStream);
        _logger = _loggingSource.GetLogger("", "");
    }
    
    [GlobalCleanup]
    public void CleanUp()
    {
        _myStream.Dispose();
    }

    public enum TestTypeEnum
    {
        Regular,
        InfoDirectlyToStream,
        UseArrayPool,
        UseArrayPoolWithStackAllocatedBuffer
    }

    
    [Params(TestTypeEnum.Regular, 
        TestTypeEnum.InfoDirectlyToStream,
        TestTypeEnum.UseArrayPool,
        TestTypeEnum.UseArrayPoolWithStackAllocatedBuffer)]
    public TestTypeEnum TestType;
    
    [Benchmark]
    public void NoParameters()
    {
        switch (TestType)
        {
            case TestTypeEnum.Regular:
                for (int j = 0; j < 1000; j++)
                    _logger.Info($"aaaaaaaaaaaaaaaaaaaaaaaaa");
                break;
            case TestTypeEnum.InfoDirectlyToStream:
                for (int j = 0; j < 1000; j++)
                    _logger.InfoDirectlyToStream((s, t) => s.InterpolateDirectly($"aaaaaaaaaaaaaaaaaaaaaaaaa"),
                        (i1, i2, i3, i4, i5)
                    );
                break;
            case TestTypeEnum.UseArrayPool:
                for (int j = 0; j < 1000; j++)
                    _logger.UseArrayPool($"aaaaaaaaaaaaaaaaaaaaaaaaa");

                break;
            case TestTypeEnum.UseArrayPoolWithStackAllocatedBuffer:
                Span<char> initBuffer = stackalloc char[128];
                for (int j = 0; j < 1000; j++)
                    _logger.UseArrayPool(initBuffer,$"aaaaaaaaaaaaaaaaaaaaaaaaa");
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }
    
    [Benchmark]
    public void IntParameter()
    {
        int a = GetInt();
        switch (TestType)
        {
            case TestTypeEnum.Regular:
                for (int j = 0; j < 1000; j++)
                    _logger.Info($"{a}aaaaaaaaaaaaaaaaaaaaaaaaa");
                break;
            case TestTypeEnum.InfoDirectlyToStream:
                for (int j = 0; j < 1000; j++)
                    _logger.InfoDirectlyToStream((s, t) => s.InterpolateDirectly($"{a}aaaaaaaaaaaaaaaaaaaaaaaaa"),
                        (i1, i2, i3, i4, i5)
                    );
                break;
            case TestTypeEnum.UseArrayPool:
                for (int j = 0; j < 1000; j++)
                    _logger.UseArrayPool($"{a}aaaaaaaaaaaaaaaaaaaaaaaaa");

                break;
            case TestTypeEnum.UseArrayPoolWithStackAllocatedBuffer:
                Span<char> initBuffer = stackalloc char[128];
                for (int j = 0; j < 1000; j++)
                    _logger.UseArrayPool(initBuffer,$"{a}aaaaaaaaaaaaaaaaaaaaaaaaa");
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    private int GetInt() => 1;

    
    int i1 = 1203;
    int i2 = 1204;
    int i3 = 1205;
    int i4 = 1206;
    int i5 = 1207;
    
    [Benchmark]
    public void MultipleIntParameter()
    {
        int a = GetInt();
        switch (TestType)
        {
            case TestTypeEnum.Regular:
                for (int j = 0; j < 1000; j++)
                    _logger.Info($"{i1}aaaaaaaaaaaaaaaa{i2}aaaaaaaaaaaaaaaaaa{i3}aaaaaaaaaaaaaaaaaaa{i4}aaaaaaaaaa{i5}aaaaaaaaaaaa");
                break;
            case TestTypeEnum.InfoDirectlyToStream:
                for (int j = 0; j < 1000; j++)
                    _logger.InfoDirectlyToStream((s, t) => s.InterpolateDirectly($"{i1}aaaaaaaaaaaaaaaa{i2}aaaaaaaaaaaaaaaaaa{i3}aaaaaaaaaaaaaaaaaaa{i4}aaaaaaaaaa{i5}aaaaaaaaaaaa"),
                        (i1, i2, i3, i4, i5)
                    );
                break;
            case TestTypeEnum.UseArrayPool:
                for (int j = 0; j < 1000; j++)
                    _logger.UseArrayPool($"{i1}aaaaaaaaaaaaaaaa{i2}aaaaaaaaaaaaaaaaaa{i3}aaaaaaaaaaaaaaaaaaa{i4}aaaaaaaaaa{i5}aaaaaaaaaaaa");

                break;
            case TestTypeEnum.UseArrayPoolWithStackAllocatedBuffer:
                Span<char> initBuffer = stackalloc char[128];
                for (int j = 0; j < 1000; j++)
                    _logger.UseArrayPool(initBuffer,$"{a}aaaaaaaaaaaaaaaaaaaaaaaaa");
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }
    
    struct MyStruct
    {
        private unsafe fixed byte ArrayPool[1024];
    }
    
    [Benchmark]
    public void BigStructParameter()
    {
        var i = new MyStruct();

        switch (TestType)
        {
            case TestTypeEnum.Regular:
                for (int j = 0; j < 1000; j++)
                    _logger.Info($"{i}aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
                break;
            case TestTypeEnum.InfoDirectlyToStream:
                for (int j = 0; j < 1000; j++)
                    _logger.InfoDirectlyToStream((s, t) => s.InterpolateDirectly($"{i}aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                        (i1, i2, i3, i4, i5)
                    );
                break;
            case TestTypeEnum.UseArrayPool:
                for (int j = 0; j < 1000; j++)
                    _logger.UseArrayPool($"{i}aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

                break;
            case TestTypeEnum.UseArrayPoolWithStackAllocatedBuffer:
                Span<char> initBuffer = stackalloc char[128];
                for (int j = 0; j < 1000; j++)
                    _logger.UseArrayPool(initBuffer,$"{i}aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    class MyObj
    {
        
    }
    
    [Benchmark]
    public void ClassParameter()
    {
        var i = new MyObj();
        switch (TestType)
        {
            case TestTypeEnum.Regular:
                for (int j = 0; j < 1000; j++)
                    _logger.Info($"{i}aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
                break;
            case TestTypeEnum.InfoDirectlyToStream:
                for (int j = 0; j < 1000; j++)
                    _logger.InfoDirectlyToStream((s, t) => s.InterpolateDirectly($"{i}aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                        (i1, i2, i3, i4, i5)
                    );
                break;
            case TestTypeEnum.UseArrayPool:
                for (int j = 0; j < 1000; j++)
                    _logger.UseArrayPool($"{i}aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

                break;
            case TestTypeEnum.UseArrayPoolWithStackAllocatedBuffer:
                Span<char> initBuffer = stackalloc char[128];
                for (int j = 0; j < 1000; j++)
                    _logger.UseArrayPool(initBuffer,$"{i}aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }
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
}
