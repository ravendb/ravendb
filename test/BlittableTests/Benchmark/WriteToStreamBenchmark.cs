using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Bond;
using Microsoft.AspNet.Mvc.Razor;
using NewBlittable;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using Raven.Server.Json;
using Marshal = System.Runtime.InteropServices.Marshal;

namespace BlittableTests.Benchmark
{
    public class WriteToStreamBenchmark
    {
        public static void ManySmallDocs(string directory, int take)
        {
            var files = Directory.GetFiles(directory, "*.json").OrderBy(f => new FileInfo(f).Length).Take(take);
            foreach (var jsonFile in files)
            {
                Console.Write(Path.GetFileName(jsonFile));
                var sp = Stopwatch.StartNew();
                int lines = 0;
                using (var reader = File.OpenText(jsonFile))
                {
                    string line;
                    while ((line = reader.ReadLine()) != null)
                    {
                        lines++;
                        JObject.Load(new JsonTextReader(new StringReader(line)));
                    }
                }
                Console.Write(" lines {1:#,#} json - {0:#,#}ms ", sp.ElapsedMilliseconds, lines);

                using (var unmanagedPool = new UnmanagedBuffersPool(string.Empty))
                using (var blittableContext = new RavenOperationContext(unmanagedPool))
                {
                    sp.Restart();

                    using (var reader = File.OpenText(jsonFile))
                    {
                        string line;
                        while ((line = reader.ReadLine()) != null)
                        {
                            using (blittableContext.Read(new MemoryStream(Encoding.UTF8.GetBytes(line)),
                                line))
                            {
                            }
                        }
                    }
                    Console.WriteLine(" blit - {0:#,#}ms", sp.ElapsedMilliseconds);
                }

            }
        }

        public static unsafe void Indexing(string directory)
        {
            var jsonCache = new List<string>();
            var blitCache = new List<Tuple<IntPtr, int>>();

            var files = Directory.GetFiles(directory, "companies.json").OrderBy(f => new FileInfo(f).Length);
            foreach (var jsonFile in files)
            {
                Console.WriteLine(Path.GetFileName(jsonFile));
                using (var reader = File.OpenText(jsonFile))
                {
                    string line;
                    while ((line = reader.ReadLine()) != null)
                    {
                        jsonCache.Add(line);
                    }
                }
            
                using (var unmanagedPool = new UnmanagedBuffersPool(string.Empty))
                using (var blittableContext = new RavenOperationContext(unmanagedPool))
                {
                    foreach (var line in jsonCache)
                    {

                        using (var doc = blittableContext.Read(new MemoryStream(Encoding.UTF8.GetBytes(line)), "doc1"))
                        {
                            var ptr = Marshal.AllocHGlobal(doc.SizeInBytes);
                            doc.CopyTo((byte*) ptr);
                            blitCache.Add(Tuple.Create(ptr, doc.SizeInBytes));
                        }
                    }
                }
            }

            Console.WriteLine($"Loaded {jsonCache.Count:#,#}");

            var sp = Stopwatch.StartNew();
            //foreach (var line in jsonCache)
            //{
            //    var jsonOjbect = JObject.Load(new JsonTextReader(new StringReader(line)));
            //    jsonOjbect.Value<string>("name");
            //    jsonOjbect.Value<string>("overview");
            //    jsonOjbect.Value<JArray>("video_embeds");
            //}
            Console.WriteLine($"Json indexing time {sp.ElapsedMilliseconds:#,#;;0}");
            sp.Restart();
            for (int i = 0; i < 30; i++)
            {
                BlitIndexing(blitCache);
            }
            Console.WriteLine($"Blit indexing time {sp.ElapsedMilliseconds:#,#;;0}");
        }

        private static unsafe void BlitIndexing(List<Tuple<IntPtr, int>> blitCache)
        {
            using (var unmanagedPool = new UnmanagedBuffersPool(string.Empty))
            using (var blittableContext = new RavenOperationContext(unmanagedPool))
            {
                foreach (var tuple in blitCache)
                {
                    var doc = new BlittableJsonReaderObject((byte*) tuple.Item1, tuple.Item2, blittableContext);
                    object result;
                    if (doc.TryGetMember("name", out result) == false)
                        throw new InvalidOperationException();
                    if (doc.TryGetMember("overview", out result) == false)
                        throw new InvalidOperationException();
                    if (doc.TryGetMember("video_embeds", out result) == false)
                        throw new InvalidOperationException();
                }
            }
        }


        public unsafe static void PerformanceAnalysis(string directory, string outputFile, int size)
        {
            using (var fileStream = new FileStream(outputFile, FileMode.Create))
            using (var streamWriter = new StreamWriter(fileStream))
            {
                var files = Directory.GetFiles(directory, "*.json").OrderBy(f => new FileInfo(f).Length).Take(size);

                streamWriter.WriteLine("Name,Json Parse Time,Json Size, Json Time, Blit Parse Time,Blit Size, Blit Time");
                using (var unmanagedPool = new UnmanagedBuffersPool(string.Empty))
                using (var blittableContext = new RavenOperationContext(unmanagedPool))
                {
                    foreach (var jsonFile in files)
                    {
                        Console.Write(string.Format("{0} {1:#,#}", Path.GetFileName(jsonFile), new FileInfo(jsonFile).Length) );
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
                        using (var employee = blittableContext.Read(File.OpenRead(jsonFile), "doc1"))
                        {
                            streamWriter.Write(sp.ElapsedMilliseconds + ",");
                            var ptr = (byte*)Marshal.AllocHGlobal(employee.SizeInBytes);
                            employee.CopyTo(ptr);
                            using (var stream = new FileStream("output2.junk", FileMode.Create))
                            {
                                sp.Restart();
                                var obj = new BlittableJsonReaderObject(ptr, employee.SizeInBytes, blittableContext);
                                obj.WriteTo(stream);
                                streamWriter.Write(employee.SizeInBytes + "," + sp.ElapsedMilliseconds + ",");
                            }
                            Marshal.FreeHGlobal((IntPtr)ptr);
                            Console.WriteLine(" blit - {0:#,#} ms, Props: {1}, Compressed: {2:#,#}/{3:#,#}",
                                sp.ElapsedMilliseconds,
                                blittableContext.CachedProperties.PropertiesDiscovered,
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
