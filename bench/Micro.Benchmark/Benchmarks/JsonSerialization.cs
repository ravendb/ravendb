using System;
using System.Collections.Generic;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Jobs;
using Raven.Client.Documents.Conventions;
using Raven.Client.Json.Serialization;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Raven.Client.Json.Serialization.SystemTextJson;
using Sparrow.Json;

namespace Micro.Benchmark.Benchmarks
{
    [MemoryDiagnoser]
    [Config(typeof(Config))]
    public class JsonSerialization
    {
        private class Config : ManualConfig
        {
            public Config()
            {
                AddJob(Job.ShortRun);
            }
        }

        private DocumentConventions _newtonsoftConventions;
        private DocumentConventions _stjConventions;
        private JsonOperationContext _context;

        private SmallEntity _smallEntity;
        private MediumEntity _mediumEntity;
        private LargeEntity _largeEntity;

        private BlittableJsonReaderObject _smallBlittable;
        private BlittableJsonReaderObject _mediumBlittable;
        private BlittableJsonReaderObject _largeBlittable;

        [GlobalSetup]
        public void Setup()
        {
            _newtonsoftConventions = new DocumentConventions
            {
                Serialization = new NewtonsoftJsonSerializationConventions()
            };

            _stjConventions = new DocumentConventions
            {
                Serialization = new SystemTextJsonSerializationConventions()
            };

            _context = JsonOperationContext.ShortTermSingleUse();

            _smallEntity = new SmallEntity
            {
                Name = "Test User",
                Age = 42,
                IsActive = true,
                CreatedAt = new DateTime(2026, 4, 11, 12, 0, 0, DateTimeKind.Utc),
                Score = 98.6
            };

            _mediumEntity = new MediumEntity
            {
                Id = "orders/1",
                CustomerName = "John Doe",
                Email = "john@example.com",
                OrderDate = new DateTime(2026, 4, 11, 12, 0, 0, DateTimeKind.Utc),
                ShippingDate = new DateTimeOffset(2026, 4, 15, 12, 0, 0, TimeSpan.Zero),
                Total = 1234.56m,
                Tax = 123.46m,
                Discount = 50.00m,
                IsShipped = false,
                Notes = "Please deliver before noon",
                ShippingAddress = new Address
                {
                    Street = "123 Main St",
                    City = "Springfield",
                    State = "IL",
                    ZipCode = "62701",
                    Country = "US"
                },
                Tags = new List<string> { "priority", "fragile", "gift-wrap" }
            };

            _largeEntity = new LargeEntity
            {
                Id = "reports/1",
                Title = "Annual Sales Report",
                GeneratedAt = new DateTime(2026, 4, 11, 12, 0, 0, DateTimeKind.Utc),
                Items = new List<ReportItem>()
            };
            for (int i = 0; i < 100; i++)
            {
                _largeEntity.Items.Add(new ReportItem
                {
                    ProductName = $"Product {i}",
                    Quantity = i * 10,
                    UnitPrice = 9.99m + i,
                    TotalPrice = (9.99m + i) * (i * 10),
                    Category = i % 5 == 0 ? "Electronics" : i % 3 == 0 ? "Clothing" : "Food"
                });
            }

            // Pre-create blittable documents for deserialization benchmarks using Newtonsoft
            var converter = _newtonsoftConventions.Serialization.DefaultConverter;
            _smallBlittable = converter.ToBlittable(_smallEntity, _context);
            _mediumBlittable = converter.ToBlittable(_mediumEntity, _context);
            _largeBlittable = converter.ToBlittable(_largeEntity, _context);
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _context?.Dispose();
        }

        // === SERIALIZATION (Entity → Blittable) ===

        [Benchmark(Description = "Newtonsoft Serialize Small")]
        public BlittableJsonReaderObject Newtonsoft_Serialize_Small()
        {
            return _newtonsoftConventions.Serialization.DefaultConverter.ToBlittable(_smallEntity, _context);
        }

        [Benchmark(Description = "STJ Serialize Small")]
        public BlittableJsonReaderObject STJ_Serialize_Small()
        {
            return _stjConventions.Serialization.DefaultConverter.ToBlittable(_smallEntity, _context);
        }

        [Benchmark(Description = "Newtonsoft Serialize Medium")]
        public BlittableJsonReaderObject Newtonsoft_Serialize_Medium()
        {
            return _newtonsoftConventions.Serialization.DefaultConverter.ToBlittable(_mediumEntity, _context);
        }

        [Benchmark(Description = "STJ Serialize Medium")]
        public BlittableJsonReaderObject STJ_Serialize_Medium()
        {
            return _stjConventions.Serialization.DefaultConverter.ToBlittable(_mediumEntity, _context);
        }

        [Benchmark(Description = "Newtonsoft Serialize Large")]
        public BlittableJsonReaderObject Newtonsoft_Serialize_Large()
        {
            return _newtonsoftConventions.Serialization.DefaultConverter.ToBlittable(_largeEntity, _context);
        }

        [Benchmark(Description = "STJ Serialize Large")]
        public BlittableJsonReaderObject STJ_Serialize_Large()
        {
            return _stjConventions.Serialization.DefaultConverter.ToBlittable(_largeEntity, _context);
        }

        // === DESERIALIZATION (Blittable → Entity) ===

        [Benchmark(Description = "Newtonsoft Deserialize Small")]
        public SmallEntity Newtonsoft_Deserialize_Small()
        {
            return _newtonsoftConventions.Serialization.DeserializeEntityFromBlittable<SmallEntity>(_smallBlittable);
        }

        [Benchmark(Description = "STJ Deserialize Small")]
        public SmallEntity STJ_Deserialize_Small()
        {
            return _stjConventions.Serialization.DeserializeEntityFromBlittable<SmallEntity>(_smallBlittable);
        }

        [Benchmark(Description = "Newtonsoft Deserialize Medium")]
        public MediumEntity Newtonsoft_Deserialize_Medium()
        {
            return _newtonsoftConventions.Serialization.DeserializeEntityFromBlittable<MediumEntity>(_mediumBlittable);
        }

        [Benchmark(Description = "STJ Deserialize Medium")]
        public MediumEntity STJ_Deserialize_Medium()
        {
            return _stjConventions.Serialization.DeserializeEntityFromBlittable<MediumEntity>(_mediumBlittable);
        }

        [Benchmark(Description = "Newtonsoft Deserialize Large")]
        public LargeEntity Newtonsoft_Deserialize_Large()
        {
            return _newtonsoftConventions.Serialization.DeserializeEntityFromBlittable<LargeEntity>(_largeBlittable);
        }

        [Benchmark(Description = "STJ Deserialize Large")]
        public LargeEntity STJ_Deserialize_Large()
        {
            return _stjConventions.Serialization.DeserializeEntityFromBlittable<LargeEntity>(_largeBlittable);
        }

        // === Entity Classes ===

        public class SmallEntity
        {
            public string Name { get; set; }
            public int Age { get; set; }
            public bool IsActive { get; set; }
            public DateTime CreatedAt { get; set; }
            public double Score { get; set; }
        }

        public class MediumEntity
        {
            public string Id { get; set; }
            public string CustomerName { get; set; }
            public string Email { get; set; }
            public DateTime OrderDate { get; set; }
            public DateTimeOffset ShippingDate { get; set; }
            public decimal Total { get; set; }
            public decimal Tax { get; set; }
            public decimal Discount { get; set; }
            public bool IsShipped { get; set; }
            public string Notes { get; set; }
            public Address ShippingAddress { get; set; }
            public List<string> Tags { get; set; }
        }

        public class Address
        {
            public string Street { get; set; }
            public string City { get; set; }
            public string State { get; set; }
            public string ZipCode { get; set; }
            public string Country { get; set; }
        }

        public class LargeEntity
        {
            public string Id { get; set; }
            public string Title { get; set; }
            public DateTime GeneratedAt { get; set; }
            public List<ReportItem> Items { get; set; }
        }

        public class ReportItem
        {
            public string ProductName { get; set; }
            public int Quantity { get; set; }
            public decimal UnitPrice { get; set; }
            public decimal TotalPrice { get; set; }
            public string Category { get; set; }
        }
    }
}
