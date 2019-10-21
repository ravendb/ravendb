using System.Runtime.CompilerServices;
using Xunit.Abstractions;

namespace FastTests
{
    public abstract class NoDisposalNeeded : XunitLoggingBase
    {
        static NoDisposalNeeded()
        {
            XunitLogging.EnableExceptionCapture();
        }

        protected NoDisposalNeeded(ITestOutputHelper output, [CallerFilePath] string sourceFile = "") : base(output, sourceFile)
        {
        }
    }
}