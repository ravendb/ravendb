using System;
using System.Diagnostics;
using Raven.Client.Documents.Conventions;
using Raven.Client.Json.Serialization.SystemTextJson;
using Sparrow.Json;

namespace Micro.Benchmark.Benchmarks
{
    public static class JsonSerializationProfile
    {
        public static void Run()
        {
            var conventions = new DocumentConventions
            {
                Serialization = new SystemTextJsonSerializationConventions()
            };

            var converter = conventions.Serialization.DefaultConverter;

            using var context = JsonOperationContext.ShortTermSingleUse();

            var entity = new SmallEntity
            {
                Name = "Test User",
                Age = 42,
                IsActive = true,
                CreatedAt = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc),
                Score = 98.6
            };

            int iterations = 200_000;

            // Warmup
            for (int i = 0; i < 1000; i++)
            {
                using var blittable = converter.ToBlittable(entity, context);
            }

            Console.WriteLine($"Starting {iterations} serialize iterations...");

            var sw = Stopwatch.StartNew();
            for (int i = 0; i < iterations; i++)
            {
                using var blittable = converter.ToBlittable(entity, context);
            }
            sw.Stop();
            Console.WriteLine($"{iterations} iterations: {sw.ElapsedMilliseconds}ms ({sw.ElapsedMilliseconds * 1000.0 / iterations:F2} us/op)");
        }

        public class SmallEntity
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public int Age { get; set; }
            public bool IsActive { get; set; }
            public DateTime CreatedAt { get; set; }
            public double Score { get; set; }
        }
    }
}
