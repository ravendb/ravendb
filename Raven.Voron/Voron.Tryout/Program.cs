using System;
using System.Threading;
using Voron.Tests.Backups;
using System.IO;
using Voron.Platform.Posix;
using System.Runtime.InteropServices;
using System.Diagnostics;
using Voron.Tests.ScratchBuffer;
using Voron.Impl.Paging;
using Voron.Tests.Journal;
using System.Linq;
using Voron.Tests;
using Xunit;
using System.Threading.Tasks;
using Voron.Tests.Bugs;
using Voron.Tests.Storage;

namespace Voron.Tryout
{
    public unsafe class Program
    {
        public static int Main()
        {
            var sp = Stopwatch.StartNew();
            var storageEnvironmentOptions = StorageEnvironmentOptions.ForPath(@"\\10.0.0.10\Documents\main");
            using (var se = new StorageEnvironment(storageEnvironmentOptions))
            {

            }
            Console.WriteLine(sp.Elapsed);
            return 0;
        }


        static void RunAllTests()
        {
            using (var fileWriter = new StreamWriter("unit-tests.txt", append: false))
            {
                var testAssembly = typeof(StorageTest).Assembly;
                var allTestClassTypes = testAssembly.GetTypes().Where(t => t.IsSubclassOf(typeof(StorageTest))).ToList();
                var allTestMethods = allTestClassTypes.SelectMany(t => t.GetMethods().Where(mt => mt.GetCustomAttributes(true)
                                                                                               .OfType<FactAttribute>().Any()))
                    .OrderBy(x => x.DeclaringType.Name + " " + x.Name)
                    .ToList();
                var total = allTestMethods.Count;
                var failed = 0;
                Console.Clear();
                fileWriter.WriteLine("found " + total + " tests to run..");
                Console.WriteLine("found " + total + " tests to run..");
                foreach (var classType in allTestClassTypes)
                {
                    foreach (var testMethod in classType.GetMethods()
                                                        .Where(mt =>
                                                            mt.GetCustomAttributes(true).OfType<FactAttribute>().Any())
                                                        .ToList())
                    {
                        Console.Write("Running test: " + testMethod.Name + "...");
                        bool isFailed = false;
                        //create new test class instance for each unit test method - just like unit test runner does
                        var testClassInstance = classType.GetConstructor(Type.EmptyTypes).Invoke(null);
                        try
                        {
                            var sw = Stopwatch.StartNew();
                            fileWriter.Write("Running test: " + testMethod.Name + "...");
                            try
                            {
                                var testMethodTask = Task.Run(() => testMethod.Invoke(testClassInstance, null));
                                if (!testMethodTask.Wait(10000))
                                {
                                    throw new TimeoutException("The test " + testMethod + " has timed-out. Aborting execution");
                                }
                            }
                            catch (Exception e)
                            {
                                fileWriter.WriteLine("Test failed. \n Reason: " + e);
                                failed++;
                                isFailed = true;
                            }
                            fileWriter.WriteLine("done. " + sw.ElapsedMilliseconds + "ms");
                            fileWriter.WriteLine("-----------------------------------------------------------");
                        }
                        finally
                        {
                            classType.GetMethod("Dispose").Invoke(testClassInstance, null);
                        }
                        if (isFailed)
                            Console.WriteLine("failed");
                        else
                            Console.WriteLine("succeeded");
                    }
                }
                fileWriter.WriteLine("------------------------------------------------");
                fileWriter.WriteLine("------------------------------------------------");
                fileWriter.WriteLine("Out of total " + total + ", failed: " + failed);
                fileWriter.Close();
                Console.WriteLine("Out of total " + total + ", failed: " + failed);
            }
        }

        static void TestEdgeCases()
        {
            using (var test = new EdgeCases())
            {
                test.TransactionCommitShouldSetCurrentLogFileToNullIfItIsFull();
            }
            Console.WriteLine("done..");
        }

        static void TestPageFileBacked()
        {
            if (File.Exists("test.map"))
                File.Delete("test.map");
            long initial = 4096;
            using (var pager = new PosixTempMemoryMapPager("test.map", initial))
            {
                for (long size = initial; size < initial * 10000; size += 4096)
                {
                    Console.WriteLine(size);
                    pager.AllocateMorePages(null, size);
                    pager.EnsureContinuous(null, 0, (int) size / AbstractPager.PageSize);
                    var p = pager.AcquirePagePointer(null, 0);
                    for (int i = 0; i < size; i++)
                    {
                        *(p + i) = 1;
                    }
                }
            }
        }

        static void ScratchBufferGrowthTest()
        {
            using (var test = new MutipleScratchBuffersUsage())
            {
                test.CanAddContinuallyGrowingValue();
            }
            Console.WriteLine("done..");
        }

        static void TestMemoryPager()
        {
            if (File.Exists("test.p"))
                File.Delete("test.p");
            var pager = new PosixMemoryMapPager("test.p");
            pager.EnsureContinuous(null, 0, 150);
            var p = pager.AcquirePagePointer(null, 0);
            for (int i = 0; i < 4096 * 150; i++)
            {
                *(p + i) = 1;
            }
            Console.WriteLine("don");
        }


        public class TestMethodRunnerCallback : ITestMethodRunnerCallback
        {
            int index;
            public int FailedCount { get; private set; }

            public void AssemblyFinished(TestAssembly testAssembly, int total, int failed, int skipped, double time)
            {
                Console.WriteLine(testAssembly.AssemblyFilename + " Total: " + total + " Failed: " + failed + " Skipped: " + skipped + " in " + time);
                FailedCount = failed;
            }

            public void AssemblyStart(TestAssembly testAssembly)
            {
                Console.WriteLine("Starting: " + testAssembly);
            }

            public bool ClassFailed(TestClass testClass, string exceptionType, string message, string stackTrace)
            {
                Console.WriteLine("Class failed: " + testClass + " - " + message);
                return true;
            }

            public void ExceptionThrown(TestAssembly testAssembly, Exception exception)
            {
                Console.WriteLine(exception);
            }

            public bool TestFinished(TestMethod testMethod)
            {
                return true;
            }

            public bool TestStart(TestMethod testMethod)
            {
                Console.WriteLine("{0,4}: {1}", ++index, testMethod.DisplayName);
                return true;
            }
        }
    }
}
