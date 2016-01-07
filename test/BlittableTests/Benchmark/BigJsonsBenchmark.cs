using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using ConsoleApplication4;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;

namespace NewBlittable.Tests.Benchmark
{
    /*public class BigJsonsBenchmark
    {
        public static void PerformanceAnalysis(string directory = @"C:\Users\bumax_000\Downloads\JsonExamples", string outputFile = @"C:\Users\bumax_000\Downloads\JsonExamples\output.csv")
        {
            using(var fs = new StreamWriter("out.csv"))
            using (var byteInAllHeapsConter = new System.Diagnostics.PerformanceCounters(
                ".NET CLR Memory", "# Bytes in all Heaps", Process.GetCurrentProcess().ProcessName))
            using (var processProcessorTimeCounter = new PerformanceCounter(
                "Process", "% Processor Time", Process.GetCurrentProcess().ProcessName))
            using (var processPrivateBytesCounter = new PerformanceCounter(
                "Process", "Private Bytes", Process.GetCurrentProcess().ProcessName))
            {
                var counters = new List<PerformanceCounter>()
                {
                    byteInAllHeapsConter,
                    processPrivateBytesCounter
                };
                var files = Directory.GetFiles(directory, "*.json3");

                fs.WriteLine("Name,Size on Disk,Json Load Time,.NET Mem,Process Mem, Blit Load Time,.NET Mem,Process Mem,Json Size, Blit Size");
                using (var unmanagedPool = new UnmanagedBuffersPool(string.Empty, 1024 * 1024 * 1024))
                using (var blittableContext = new BlittableContext(unmanagedPool))
                {
                    foreach (var jsonFile in files)
                    {
                        fs.Write(Path.GetFileName(jsonFile) +",");
                        var jsonFileText = File.ReadAllText(jsonFile);
                        fs.Write(new FileInfo(jsonFile).Length +",");
                        GC.Collect(2);
                        var result = JsonProcessorRunner(() =>
                            JObject.Load(new JsonTextReader(new StringReader(jsonFileText))), counters, 50);
                        GC.Collect(2);
                        fs.Write(result.Duration+",");
                        foreach (var countersValue in result.CountersValues)
                        {
                            fs.Write(countersValue.Value.Max(x => x.RawValue) +",");
                        }
                        GC.Collect(2);
                        result = JsonProcessorRunner(() =>
                        {
                            using (
                                var employee =
                                    new BlittableJsonWriter(new JsonTextReader(new StringReader(jsonFileText)),
                                        blittableContext,
                                        "doc1"))
                            {
                                employee.Write();
                            }
                        }, counters, 50);
                        GC.Collect(2);
                        fs.Write(result.Duration+",");
                        Console.WriteLine(result.Duration);
                        foreach (var countersValue in result.CountersValues)
                        {
                            Console.WriteLine(countersValue.Key);
                            fs.Write(countersValue.Value.Average(x => x.RawValue) + ",");
                        }
                        var size =
                            JObject.Load(new JsonTextReader(new StringReader(jsonFileText)))
                                .ToString(Formatting.None)
                                .Length;
                        fs.Write(size+",");
                        using (
                                var employee =
                                    new BlittableJsonWriter(new JsonTextReader(new StringReader(jsonFileText)),
                                        blittableContext,
                                        "doc1"))
                        {
                            employee.Write();
                            fs.Write(employee.SizeInBytes+",");
                        }
                        fs.WriteLine();
                    }
                }
            }
        }

        public class OperationResults
        {
            public ConcurrentDictionary<string, List<CounterSample>> CountersValues;
            public long Duration;
        }

        public static OperationResults JsonProcessorRunner(Action processor, List<PerformanceCounter> counters,
            int resolutionInMiliseconds)
        {
            var sp = Stopwatch.StartNew();
            var results = new OperationResults
            {
                CountersValues = new ConcurrentDictionary<string, List<CounterSample>>()
            };
            int signaled = 0;
            foreach (var counter in counters)
            {
                results.CountersValues.TryAdd($"{counter.CategoryName}\\{counter.CounterName}",
                    new List<CounterSample>());

            }
            var jsonProcessorTask = Task.Run(processor).ContinueWith(x => Interlocked.Exchange(ref signaled, 1));
            var countersCollectorTask = Task.Run(() =>
            {
                while (Interlocked.CompareExchange(ref signaled, 1, 1) == 0)
                {
                    Parallel.ForEach(counters, counter =>
                    {
                        results.CountersValues[$"{counter.CategoryName}\\{counter.CounterName}"].Add(
                            counter.NextSample());
                    });
                    Thread.Sleep(resolutionInMiliseconds);
                }
            });
            Task.WaitAll(new[] { jsonProcessorTask, countersCollectorTask });
            results.Duration = sp.ElapsedMilliseconds;
            return results;
        }
    }*/
}