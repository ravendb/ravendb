using Microsoft.Xunit.Performance;
using Sparrow.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace Regression
{
    public class BlittableJsonBench : BenchBase
    {
        [Benchmark]
        public void ParseJsonFromStream()
        {
            var streams = new List<Tuple<string, Stream>>();
            foreach (var name in new[] { "1.json", "2.json", "3.json", "monsters.json" })
            {
                var resource = "Regression.Benchmark.Data." + name;

                streams.Add(new Tuple<string, Stream>("id/" + name, typeof(BlittableJsonBench).GetTypeInfo().Assembly.GetManifestResourceStream(resource)));
            }

            ExecuteBenchmark(() =>
            {
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {

                    foreach (var tuple in streams)
                    {
                        // We parse the whole thing.
                        var obj = context.Read(tuple.Item2, tuple.Item1);

                        // Perform validation (Include when fixed)
                        obj.BlittableValidation();                       
                    }
                }
            });
        }

        [Benchmark]
        public void WriteJsonFromStream()
        {
            var objects = new List<BlittableJsonReaderObject>();
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                foreach (var name in new[] { "1.json", "2.json", "3.json", "monsters.json" })
                {                
                    var resource = "Regression.Benchmark.Data." + name;

                    using (var stream = typeof(BlittableJsonBench).GetTypeInfo().Assembly.GetManifestResourceStream(resource))
                    {
                        // We parse the whole thing.
                        var obj = context.Read(stream, "id/" + name);
                        objects.Add(obj);
                    }
                }

                var memoryStream = new MemoryStream();

                ExecuteBenchmark(() =>
                {

                        foreach (var obj in objects)
                        {
                            // We write the whole thing.
                            context.Write(memoryStream, obj);
                        }

                });
            }
        }
    }
}
