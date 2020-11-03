using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Xunit.Performance;
using Sparrow.Json;

namespace Regression.Benchmark
{
    public class BlittableJsonBench : BenchBase
    {
        [Benchmark]
        public async Task ParseJsonFromStream()
        {
            var streams = new List<Tuple<string, Stream>>();
            foreach (var name in new[] { "1.json", "2.json", "3.json", "monsters.json" })
            {
                var resource = "Regression.Benchmark.Data." + name;

                streams.Add(new Tuple<string, Stream>("id/" + name, typeof(BlittableJsonBench).Assembly.GetManifestResourceStream(resource)));
            }

            await ExecuteBenchmarkAsync(async () =>
            {
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    foreach (var tuple in streams)
                    {
                        // We parse the whole thing.
                        var obj = await context.ReadForDiskAsync(tuple.Item2, tuple.Item1);

                        // Perform validation (Include when fixed)
                        obj.BlittableValidation();
                    }
                }
            });
        }

        [Benchmark]
        public async Task WriteJsonFromStream()
        {
            List<BlittableJsonReaderObject> objects;
            objects = new List<BlittableJsonReaderObject>();
            try
            {
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    foreach (var name in new[] { "1.json", "2.json", "3.json", "monsters.json" })
                    {
                        var resource = "Regression.Benchmark.Data." + name;

                        using (var stream = typeof(BlittableJsonBench).Assembly.GetManifestResourceStream(resource))
                        {
                            // We parse the whole thing.
                            var obj = await context.ReadForDiskAsync(stream, "id/" + name);
                            objects.Add(obj);
                        }
                    }

                    var memoryStream = new MemoryStream();

                    await ExecuteBenchmarkAsync(async () =>
                    {
                        foreach (var obj in objects)
                        {
                            // We write the whole thing.
                            await context.WriteAsync(memoryStream, obj);
                        }
                    });
                }
            }
            finally
            {
                foreach (var doc in objects)
                    doc.Dispose();
            }
        }
    }
}
