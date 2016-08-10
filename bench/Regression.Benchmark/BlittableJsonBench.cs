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
            foreach (var name in new[] { "1.json", "2.json", "3.json", "monsters.json" })
            {
                using (var context = new JsonOperationContext())
                {
                    var resource = "Regression.Benchmark.Data." + name;

                    using (var stream = typeof(BlittableJsonBench).GetTypeInfo().Assembly
                        .GetManifestResourceStream(resource))
                    {
                        ExecuteBenchmark(() =>
                        {
                            // We parse the whole thing.
                            var obj = context.Read(stream, "id/" + name);

                            // Perform validation (Include when fixed)
                            // obj.BlittableValidation();
                        });
                    }
                }
            }
        }

        [Benchmark]
        public void WriteJsonFromStream()
        {
            foreach (var name in new[] { "1.json", "2.json", "3.json", "monsters.json" })
            {
                using (var context = new JsonOperationContext())
                {
                    var resource = "Regression.Benchmark.Data." + name;

                    using (var stream = typeof(BlittableJsonBench).GetTypeInfo().Assembly
                        .GetManifestResourceStream(resource))
                    {
                        // We parse the whole thing.
                        var obj = context.Read(stream, "id/" + name);

                        ExecuteBenchmark(() =>
                        {
                            // We write the whole thing.
                            var memoryStream = new MemoryStream();
                            context.Write(memoryStream, obj);
                        });
                    }
                }
            }
        }
    }
}
