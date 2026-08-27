using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;
using Sparrow.Json;

namespace Micro.Benchmark.Benchmarks.Json
{
    /// <summary>
    /// Parsing and writing of blittable JSON over a fixed corpus of documents.
    /// </summary>
    [Config(typeof(BlittableJsonBench.Config))]
    public class BlittableJsonBench
    {
        private class Config : ManualConfig
        {
            public Config()
            {
                // No Runtime pin: the job then runs on the project's target framework.
                // The CoreRuntime.Core80 used elsewhere in this project cannot be built
                // from a net10.0-only project, so those jobs always fail.
                AddJob(new Job
                {
                    Environment = { Platform = Platform.X64, Jit = Jit.RyuJit, },
                });

                AddExporter(GetExporters().ToArray());

                AddValidator(BaselineValidator.FailOnError);
                AddValidator(JitOptimizationsValidator.FailOnError);

                AddAnalyser(EnvironmentAnalyser.Default);
            }
        }

        private static readonly string[] DocumentNames = { "1.json", "2.json", "3.json", "monsters.json" };

        private readonly List<(string Id, MemoryStream Stream)> _documents = new List<(string, MemoryStream)>();

        private JsonOperationContext _writeContext;
        private List<BlittableJsonReaderObject> _objectsToWrite;
        private MemoryStream _writeDestination;

        [GlobalSetup]
        public void Setup()
        {
            // The documents are buffered rather than read straight off the manifest
            // streams, so every iteration parses the same bytes from the start.
            foreach (string name in DocumentNames)
            {
                string resource = "Micro.Benchmark.Data." + name;

                using (Stream resourceStream = typeof(BlittableJsonBench).Assembly.GetManifestResourceStream(resource))
                {
                    if (resourceStream == null)
                        throw new InvalidOperationException($"Could not find embedded resource '{resource}'.");

                    MemoryStream buffered = new MemoryStream();
                    resourceStream.CopyTo(buffered);
                    buffered.Position = 0;

                    _documents.Add(("id/" + name, buffered));
                }
            }

            _writeContext = JsonOperationContext.ShortTermSingleUse();
            _objectsToWrite = new List<BlittableJsonReaderObject>();
            _writeDestination = new MemoryStream();

            foreach ((string id, MemoryStream stream) in _documents)
            {
                stream.Position = 0;
                _objectsToWrite.Add(_writeContext.ReadForDiskAsync(stream, id).AsTask().GetAwaiter().GetResult());
            }
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            foreach (BlittableJsonReaderObject obj in _objectsToWrite)
                obj.Dispose();

            _objectsToWrite.Clear();
            _writeContext.Dispose();
            _writeDestination.Dispose();

            foreach ((string _, MemoryStream stream) in _documents)
                stream.Dispose();

            _documents.Clear();
        }

        [Benchmark]
        public async Task ParseJsonFromStream()
        {
            // The context is created and disposed inside the measured region, matching
            // what a single short-lived parse actually costs.
            using (JsonOperationContext context = JsonOperationContext.ShortTermSingleUse())
            {
                foreach ((string id, MemoryStream stream) in _documents)
                {
                    stream.Position = 0;

                    BlittableJsonReaderObject obj = await context.ReadForDiskAsync(stream, id);
                    obj.BlittableValidation();
                }
            }
        }

        [Benchmark]
        public async Task WriteJsonToStream()
        {
            _writeDestination.SetLength(0);

            foreach (BlittableJsonReaderObject obj in _objectsToWrite)
                await _writeContext.WriteAsync(_writeDestination, obj);
        }
    }
}
