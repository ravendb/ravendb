using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace FastTests.Blittable.Benchmark
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
                int size = 0;
                using (var reader = File.OpenText(jsonFile))
                {
                    string line;
                    while ((line = reader.ReadLine()) != null)
                    {
                        lines++;
                        size += line.Length;
                        JObject.Load(new JsonTextReader(new StringReader(line)));
                    }
                }
                Console.Write(" lines {1:#,#} json - {0:#,#}ms - {2:#,#} ", sp.ElapsedMilliseconds, lines, size);

                size = 0;
                using (var blittableContext = JsonOperationContext.ShortTermSingleUse())
                {
                    sp.Restart();

                    using (var reader = File.OpenText(jsonFile))
                    {
                        string line;
                        while ((line = reader.ReadLine()) != null)
                        {
                            using (var a = blittableContext.Read(new MemoryStream(Encoding.UTF8.GetBytes(line)),
                                line))
                            {
                                size += a.Size;
                            }
                        }
                    }
                    Console.WriteLine(" blit - {0:#,#}ms - {1:#,#}", sp.ElapsedMilliseconds, size);
                }

            }
        }

        public static void Indexing(string directory)
        {
            var jsonCache = new List<string>();
            var blitCache = new List<BlittableJsonReaderObject>();

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

                using (var blittableContext = JsonOperationContext.ShortTermSingleUse())
                {
                    foreach (var line in jsonCache)
                    {

                        using (var doc = blittableContext.Read(new MemoryStream(Encoding.UTF8.GetBytes(line)), "doc1"))
                        {
                            blitCache.Add(doc);
                        }
                    }
                }
            }

            Console.WriteLine($"Loaded {jsonCache.Count:#,#}");

            var sp = Stopwatch.StartNew();
            //foreach (var line in jsonCache)
            //{
            //    var jsonOjbect = JObject.Load(new JsonTextReader(new StringReader(line)));
            //    jsonOjbect.Items<string>("name");
            //    jsonOjbect.Items<string>("overview");
            //    jsonOjbect.Items<JArray>("video_embeds");
            //}
            Console.WriteLine($"Json indexing time {sp.ElapsedMilliseconds:#,#;;0}");
            sp.Restart();
            for (int i = 0; i < 30; i++)
            {
                BlitIndexing(blitCache);
            }
            Console.WriteLine($"Blit indexing time {sp.ElapsedMilliseconds:#,#;;0}");
        }

        private static unsafe void BlitIndexing(List<BlittableJsonReaderObject> blitCache)
        {
            using (var blittableContext = JsonOperationContext.ShortTermSingleUse())
            {
                foreach (var tuple in blitCache)
                {
                    var doc = new BlittableJsonReaderObject(tuple.BasePointer, tuple.Size, blittableContext);
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


        public static void PerformanceAnalysis(string directory, string outputFile, int size)
        {
            using (var fileStream = new FileStream(outputFile, FileMode.Create))
            using (var streamWriter = new StreamWriter(fileStream))
            {
                var files = Directory.GetFiles(directory, "*.json").OrderBy(f => new FileInfo(f).Length).Take(size);

                streamWriter.WriteLine("Name,Json Parse Time,Json Size, Json Time, Blit Parse Time,Blit Size, Blit Time");
                using (var blittableContext = JsonOperationContext.ShortTermSingleUse())
                {
                    foreach (var jsonFile in files)
                    {
                        Console.Write(string.Format("{0} {1:#,#}", Path.GetFileName(jsonFile), new FileInfo(jsonFile).Length));
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
                            using (var stream = new FileStream("output2.junk", FileMode.Create))
                            {
                                sp.Restart();
                                blittableContext.Write(stream, employee);
                                streamWriter.Write(employee.Size + "," + sp.ElapsedMilliseconds + ",");
                            }
                            Console.WriteLine(" blit - {0:#,#} ms, Props: {1}",
                                sp.ElapsedMilliseconds,
                                blittableContext.CachedProperties.PropertiesDiscovered);
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
