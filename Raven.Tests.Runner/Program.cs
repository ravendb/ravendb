using System;
using System.Linq;
using Xunit;

namespace Raven.Tests.Runner
{
	public class Program
	{
		public static void Main(string[] args)
		{
			Console.WriteLine("Loading assembly...");

			var testAssembly = TestAssemblyBuilder.Build(new ExecutorWrapper("Raven.Tests.dll", null, false));

			Console.WriteLine("Assembly loaded...");

			while (true)
			{
				Console.WriteLine("Please provide a test name:");
				var testNamePredicate = Console.ReadLine();
				if (string.IsNullOrEmpty(testNamePredicate))
					break;

				var testMethods = testAssembly.EnumerateTestMethods(x => x.DisplayName.ToLowerInvariant().Contains(testNamePredicate.ToLowerInvariant())).ToList();

				Console.WriteLine("Found {0} test(s) that contain: {1}", testMethods.Count, testNamePredicate);

				testAssembly.Run(testMethods, new TestMethodRunnerCallback());
			}
		}

		public class TestMethodRunnerCallback : ITestMethodRunnerCallback
		{
			public void AssemblyFinished(TestAssembly testAssembly, int total, int failed, int skipped, double time)
			{
				Console.WriteLine();
				Console.WriteLine("Finished testing. {0} tests failed, {1} tests skipped, {2} total tests, execution time {3}s", failed, skipped, total, time);

				Console.WriteLine();
				Console.WriteLine();
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
				Console.WriteLine("Test '{0}' {1}.", testMethod.DisplayName, testMethod.RunStatus == TestStatus.Passed ? "passed" : "failed");
				return true;
			}

			public bool TestStart(TestMethod testMethod)
			{
				Console.WriteLine();
				Console.WriteLine("Test '{0}' starting.", testMethod.DisplayName);
				return true;
			}
		}
	}
}
