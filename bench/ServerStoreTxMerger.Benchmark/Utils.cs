using System;
using System.Text;
using Xunit.Abstractions;

namespace ServerStoreTxMerger.Benchmark;

public class Doc
{
    public string Id { get; set; }
    public string Data { get; set; }
}

public class MyOutputHelper : ITestOutputHelper
{
    public void WriteLine(string message) => Console.WriteLine(message);

    public void WriteLine(string format, params object[] args) => Console.WriteLine(format, args);

    public void Dispose()
    {
    }
}

public class StringRandom
{
    private readonly Random _random = new Random();
    public string GetRandomData(int size)
    {
        var sb = new StringBuilder();
        int a = 'a' - 0;
        int z = 'z' - 0;
        for (int i = 0; i < size; i++)
        {
            var c = (char)(_random.Next(a, z + 1));
            sb.Append(c);
        }
        return sb.ToString();
    }
}
