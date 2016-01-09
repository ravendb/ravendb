using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using BlittableTests.Benchmark;
using NewBlittable;
using Newtonsoft.Json;
using Raven.Server.Json;

namespace Tryouts
{
    public unsafe class Program
    {
        public static void Main(string[] args)
        {
            var json = JsonConvert.SerializeObject(new
            {
                Name = "Oren",
                Dogs = new[] { "Arava", "Oscar", "Phoebe" },
                Age = 34,
                Position = 0.5f,
                Office = new
                {
                    Name = "Hibernating Rhinos",
                    Street = "Hanais 21",
                    City = "Hadera"
                }
            });

            using (var pool = new UnmanagedBuffersPool("test", 1024 * 1024))
            using (var ctx = new RavenOperationContext(pool))
            using (var obj = ctx.Read(new JsonTextReader(new StringReader(json)), "test/1"))
            {
                int size;
                var buffer = ctx.GetNativeTempBuffer(obj.SizeInBytes, out size);
                size = obj.CopyTo(buffer);
                var r = new BlittableJsonReaderObject(buffer, size, ctx);

                var ms = new MemoryStream();
                r.WriteTo(ms);
                Console.WriteLine(Encoding.UTF8.GetString(ms.ToArray()));
                Console.WriteLine(ms.ToString() == json);
            }

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
