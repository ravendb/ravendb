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
        public int LoadJsonFromStream()
        {
            int counted = 0;
            ExecuteBenchmark(() =>
            {

                foreach (var name in new[] { "1.json", "2.json", "3.json", "monsters.json" })
                {
                    using (var pool = new UnmanagedBuffersPool("test"))
                    using (var context = new JsonOperationContext(pool))
                    {
                        var resource = "Regression.Benchmark.Data." + name;

                        using (var stream = typeof(BlittableJsonBench).GetTypeInfo().Assembly
                            .GetManifestResourceStream(resource))
                        {

                            // We parse the whole thing.
                            var obj = context.Read(stream, "id/" + name);

                            // Perform validation (Include when fixed)
                            // obj.BlittableValidation();

                            // Get properties by insertion order.
                            int[] properties = obj.GetPropertiesByInsertionOrder();
                            foreach (var prop in properties)
                            {
                                var data = obj.GetPropertyByIndex(prop);
                                var convertedToString = data.Item1.ToString();

                                counted += convertedToString.Length;
                            }

                            // We write the whole thing.
                            var memoryStream = new MemoryStream();
                            context.Write(memoryStream, obj);
                        }
                    }
                }
            });        

            return counted;
        }
    }
}
