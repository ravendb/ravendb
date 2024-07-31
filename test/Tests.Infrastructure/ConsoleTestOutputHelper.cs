using System;
using Xunit.Abstractions;

namespace Tests.Infrastructure
{
    public class ConsoleTestOutputHelper : ITestOutputHelper, IDisposable
    {
        public void WriteLine(string message) => Console.WriteLine(message);

        public void WriteLine(string format, params object[] args) => Console.WriteLine(format, args);

        public void Dispose()
        {
            try
            {
                XunitLogging.Flush();
            }
            catch (Exception)
            {
            }
        }
    }
}
