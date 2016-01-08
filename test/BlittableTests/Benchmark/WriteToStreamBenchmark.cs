using System;
using System.Diagnostics;
using System.IO;
using Bond;
using Microsoft.AspNet.Mvc.Razor;
using NewBlittable;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Server.Json;
using Marshal = System.Runtime.InteropServices.Marshal;

namespace BlittableTests.Benchmark
{
    public class WriteToStreamBenchmark
    {
        public unsafe static void PerformanceAnalysis(string directory = @"C:\Users\bumax_000\Downloads\JsonExamples", string outputFile = @"C:\Users\bumax_000\Downloads\JsonExamples\output.csv")
        {
            using (var fileStream = new FileStream(outputFile, FileMode.Create))
            using (var streamWriter = new StreamWriter(fileStream))
            {
                var files = Directory.GetFiles(directory, "*.json");

                streamWriter.WriteLine("Name,Size on Disk,Json Write Time,Blit Write Time");
                using (var unmanagedPool = new UnmanagedBuffersPool(string.Empty, 1024 * 1024 * 1024))
                using (var blittableContext = new RavenOperationContext(unmanagedPool))
                {
                    foreach (var jsonFile in files)
                    {
                        streamWriter.Write(Path.GetFileName(jsonFile) + ",");
                        var jsonFileText = File.ReadAllText(jsonFile);
                        streamWriter.Write(new FileInfo(jsonFile).Length + ",");
                        GC.Collect(2);
                        //var jsonOjbect = JObject.Load(new JsonTextReader(new StringReader(jsonFileText)));
                        var inMemoryStream = new MemoryStream();
                        var result = JsonProcessorRunner(() =>
                        {
                            var jsonOjbect = JObject.Load(new JsonTextReader(new StringReader(jsonFileText)));
                            jsonOjbect.WriteTo(
                                new JsonTextWriter(new StreamWriter(new FileStream("output.junk", FileMode.Create))));
                        });
                        
                        GC.Collect(2);
                        Console.WriteLine(result.Duration);
                        streamWriter.Write(result.Duration + ",");

                        inMemoryStream.Seek(0, SeekOrigin.Begin);

                        GC.Collect(2);
                        using (
                                var employee =
                                    new BlittableJsonWriter(new JsonTextReader(new StringReader(jsonFileText)),
                                        blittableContext,
                                        "doc1"))
                        {
                            employee.Write();
                            var ptr = (byte*)Marshal.AllocHGlobal(employee.SizeInBytes);
                            employee.CopyTo(ptr);
                            inMemoryStream = new MemoryStream();
                            result = JsonProcessorRunner(() =>
                            {
                                new BlittableJsonReaderObject(ptr, employee.SizeInBytes, blittableContext).WriteTo(new StreamWriter(new FileStream("output2.junk",FileMode.Create)));
                            });
                            Marshal.FreeHGlobal((IntPtr)ptr);
                        }
                        GC.Collect(2);
                        streamWriter.Write(result.Duration );
                        Console.WriteLine(result.Duration);
                        streamWriter.WriteLine();
                    }
                }
            }
        }

        public class OperationResults
        {
            public long Duration;
        }

        public static OperationResults JsonProcessorRunner(Action processor)
        {
            var sp = Stopwatch.StartNew();
            var results = new OperationResults();
            processor();
            results.Duration = sp.ElapsedMilliseconds;
            return results;
        }
    }

}
