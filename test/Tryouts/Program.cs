using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using BlittableTests.Benchmark;
using BlittableTests.BlittableJsonWriterTests;
using NewBlittable;
using Newtonsoft.Json;
using Raven.Server.Json;

namespace Tryouts
{
    public unsafe class Program
    {
        public static void Main(string[] args)
        {
            var f = new BlittableFormatTests();
            foreach (var sample in f.Samples())
            {
                Console.WriteLine(sample);
                try
                {
                    f.CheckRoundtrip(sample);
                }
                catch (Exception e)
                {
                    Console.ForegroundColor=ConsoleColor.Red;
                    Console.WriteLine(e);
                    Console.ResetColor();
                    break;
                }
            }

            //Console.WriteLine(JsonConvert.SerializeObject(new {F = (double)decimal.MinValue}));

            //var json = JsonConvert.SerializeObject(new
            //{
            //    Name = "Oren",
            //     Dogs = new[] { "Arava", "Oscar", "Phoebe" },
            //    Age = 34,
            //    Position = 0.5f,
            //     Office = new
            //       {
            //           Name = "Hibernating Rhinos",
            //           Street = "Hanais 21",
            //           City = "Hadera"
            //       }
            //});

            //Console.WriteLine(json);
            //using (var pool = new UnmanagedBuffersPool("test", 1024 * 1024))
            //using (var ctx = new RavenOperationContext(pool))
            //using (var obj = ctx.Read(new JsonTextReader(new StringReader(json)), "test/1"))
            //{
            //    int size;
            //    var buffer = ctx.GetNativeTempBuffer(obj.SizeInBytes, out size);
            //    size = obj.CopyTo(buffer);
            //    var r = new BlittableJsonReaderObject(buffer, size, ctx);

            //    var ms = new MemoryStream();
            //    r.WriteTo(ms, originalPropertyOrder: true);
            //    var format = Encoding.UTF8.GetString(ms.ToArray());
            //    Console.WriteLine(format);
            //    Console.WriteLine(format == json);
            //}

            //WriteToStreamBenchmark.ManySmallDocs(@"C:\Work\JSON\Lines");

            //var outputFile = Path.GetTempFileName() + ".CSV";

            //WriteToStreamBenchmark.PerformanceAnalysis(@"C:\Work\JSON\Big", outputFile, int.MaxValue);

            //Console.WriteLine("Real test");

            //outputFile = Path.GetTempFileName() + ".CSV";

            //WriteToStreamBenchmark.PerformanceAnalysis(@"C:\Work\JSON\Big", outputFile, int.MaxValue);

            //Console.WriteLine(outputFile);
        }
    }

}
