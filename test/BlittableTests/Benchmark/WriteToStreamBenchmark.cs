using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
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
        public unsafe static void PerformanceAnalysis(string directory, string outputFile)
        {
            Console.WriteLine(IntPtr.Size);
            using (var fileStream = new FileStream(outputFile, FileMode.Create))
            using (var streamWriter = new StreamWriter(fileStream))
            {
                var files = Directory.GetFiles(directory, "*.json").OrderBy(f => new FileInfo(f).Length);

                streamWriter.WriteLine("Name,Json Parse Time,Json Size, Json Time, Blit Parse Time,Blit Size, Blit Time");
                using (var unmanagedPool = new UnmanagedBuffersPool(string.Empty, 1024 * 1024 * 1024))
                using (var blittableContext = new RavenOperationContext(unmanagedPool))
                {
                    foreach (var jsonFile in files)
                    {
                        Console.Write(Path.GetFileName(jsonFile));
                        streamWriter.Write(Path.GetFileName(jsonFile) + ",");
                        var sp = Stopwatch.StartNew();
                        var jsonOjbect = JObject.Load(new JsonTextReader(File.OpenText(jsonFile)));
                        streamWriter.Write(sp.ElapsedMilliseconds + ",");

                        using (var stream = new FileStream("output.junk", FileMode.Create))
                        using (var textWriter = new StreamWriter(stream))
                        {
                            sp.Restart();
                            jsonOjbect.WriteTo(new JsonTextWriter(textWriter));
                            textWriter.Flush();
                            streamWriter.Write(stream.Length + "," + sp.ElapsedMilliseconds + ",");
                        }


                        Console.Write(" json - {0:#,#}ms", sp.ElapsedMilliseconds);
                        GC.Collect(2);

                        sp.Restart();
                        using (
                            var employee =
                                new BlittableJsonWriter(new JsonTextReader(File.OpenText(jsonFile)),
                                    blittableContext,
                                    "doc1"))
                        {
                            employee.Write();
                            streamWriter.Write(sp.ElapsedMilliseconds + ",");
                            var ptr = (byte*)Marshal.AllocHGlobal(employee.SizeInBytes);
                            employee.CopyTo(ptr);
                            using (var stream = new FileStream("output2.junk", FileMode.Create))
                            using (var writer = new StreamWriter(stream))
                            using (var jsonWriter = new Raven.Imports.Newtonsoft.Json.JsonTextWriter(writer))
                            {
                                sp.Restart();
                                var obj = new BlittableJsonReaderObject(ptr, employee.SizeInBytes, blittableContext);
                                obj.WriteTo(jsonWriter);
                                streamWriter.Write(stream.Length + "," + sp.ElapsedMilliseconds + ",");
                            }
                            Marshal.FreeHGlobal((IntPtr)ptr);
                            Console.WriteLine(" blit - {0:#,#} ms, Props: {1}, Compressed: {2:#,#}/{3:#,#}",
                                sp.ElapsedMilliseconds,
                                employee.TotalNumberOfProperties,
                                employee.DiscardedCompressions,
                                employee.Compressed);
                        }
                        GC.Collect(2);

                        streamWriter.WriteLine();
                    }
                }
            }
        }

        public class OperationResults
        {
            public long Duration;
            public long Size;
        }

        public static OperationResults JsonProcessorRunner(Func<long> processor)
        {
            var sp = Stopwatch.StartNew();
            return new OperationResults
            {
                Size = processor(),
                Duration = sp.ElapsedMilliseconds
            };
        }
    }

}
