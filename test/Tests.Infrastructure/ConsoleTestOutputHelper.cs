using System;
using Xunit;

namespace Tests.Infrastructure
{
    public class ConsoleTestOutputHelper : ITestOutputHelper, IDisposable
    {
        public string Output => string.Empty;

        public void Write(string message) => Console.Write(message);

        public void Write(string format, params object[] args) => Console.Write(format, args);

        public void WriteLine(string message) => Console.WriteLine(message);

        public void WriteLine(string format, params object[] args) => Console.WriteLine(format, args);

        public void Dispose()
        {
        }
    }
}
