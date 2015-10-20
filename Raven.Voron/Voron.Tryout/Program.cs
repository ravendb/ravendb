using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Voron.Platform.Posix;
using Voron.Tests;
using Xunit;

namespace Voron.Tryout
{
    public unsafe class Program
    {
        public static int Main()
        {
            var sp = Stopwatch.StartNew();
            using (var se = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
            {
                using (var tx = se.WriteTransaction())
                {
                    tx.CreateTree("test").Add("test", "val");
                    tx.Commit();
                }

                using (var tx = se.ReadTransaction())
                {
                    Console.WriteLine(tx.ReadTree("test").Read("test").Reader.ToString());
                }
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
         
            Console.WriteLine("done..");
        }

        static void TestPageFileBacked()
        {
            if (File.Exists("test.map"))
                File.Delete("test.map");
            long initial = 4096;
            using (var pager = new PosixTempMemoryMapPager(4096, "test.map", initial))
            {
                for (long size = initial; size < initial * 10000; size += 4096)
                {
                    Console.WriteLine(size);
                    pager.EnsureContinuous(0, (int)size);
                    pager.EnsureContinuous(0, (int)size / pager.PageSize);
                    var p = pager.AcquirePagePointer(0);
                    for (int i = 0; i < size; i++)
                    {
                        *(p + i) = 1;
                    }
                }
            }
        }

        static void ScratchBufferGrowthTest()
        {
           
            Console.WriteLine("done..");
        }

        static void TestMemoryPager()
        {
            if (File.Exists("test.p"))
                File.Delete("test.p");
            var pager = new PosixMemoryMapPager(4096, "test.p");
            pager.EnsureContinuous(0, 150);
            var p = pager.AcquirePagePointer(0);
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