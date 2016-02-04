using System;
using System.Linq;

using Xunit;

namespace Raven.Xunit.Runner
{
    public class Program
    {
        public static int Main(string[] args)
        {
            var testAssembly = TestAssemblyBuilder.Build(new ExecutorWrapper(args[0], null, false));

            var testMethods = testAssembly.EnumerateTestMethods(x => x.DisplayName.ToLowerInvariant().Contains(args[1].ToLowerInvariant())).ToList();
            if (testMethods.Count == 0)
                return 0;

            var callback = new TestMethodRunnerCallback();
            testAssembly.Run(testMethods, callback);

            if (callback.FailedCount > 0)
                return 1;

            return 0;
        }

        public class TestMethodRunnerCallback : ITestMethodRunnerCallback
        {
            public int FailedCount { get; private set; }

            public void AssemblyFinished(TestAssembly testAssembly, int total, int failed, int skipped, double time)
            {
                FailedCount = failed;
            }

            public void AssemblyStart(TestAssembly testAssembly)
            {
            }

            public bool ClassFailed(TestClass testClass, string exceptionType, string message, string stackTrace)
            {
                return true;
            }

            public void ExceptionThrown(TestAssembly testAssembly, Exception exception)
            {
            }

            public bool TestFinished(TestMethod testMethod)
            {
                return true;
            }

            public bool TestStart(TestMethod testMethod)
            {
                return true;
            }
        }
    }
}
