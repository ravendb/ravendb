using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Raven.Client;
using Raven.Client.Exceptions.Documents.BulkInsert;
using Raven.Client.Extensions;
using Raven.Client.Json;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client.BulkInsert
{
    public class BulkInserts : RavenTestBase
    {
        public BulkInserts(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.BulkInsert)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void SimpleBulkInsertShouldWork(Options options)
        {
            var fooBars = new[]
            {
                new FooBar { Name = "John Doe" },
                new FooBar { Name = "Jane Doe" },
                new FooBar { Name = "Mega John" },
                new FooBar { Name = "Mega Jane" }
            };

            using (var store = GetDocumentStore(options))
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(fooBars[0]);
                    bulkInsert.Store(fooBars[1]);
                    bulkInsert.Store(fooBars[2]);
                    bulkInsert.Store(fooBars[3]);
                }

                using (var session = store.OpenSession())
                {
                    var doc1 = session.Load<FooBar>("FooBars/1-A");
                    var doc2 = session.Load<FooBar>("FooBars/2-A");
                    var doc3 = session.Load<FooBar>("FooBars/3-A");
                    var doc4 = session.Load<FooBar>("FooBars/4-A");

                    Assert.NotNull(doc1);
                    Assert.NotNull(doc2);
                    Assert.NotNull(doc3);
                    Assert.NotNull(doc4);

                    Assert.Equal("John Doe", doc1.Name);
                    Assert.Equal("Jane Doe", doc2.Name);
                    Assert.Equal("Mega John", doc3.Name);
                    Assert.Equal("Mega Jane", doc4.Name);
                }
            }
        }

        [RavenFact(RavenTestCategory.BulkInsert)]
        public async Task AsyncSimpleBulkInsertShouldWork()
        {
            var fooBars = new[]
            {
                new FooBar { Name = "John Doe" },
                new FooBar { Name = "Jane Doe" },
                new FooBar { Name = "Mega John" },
                new FooBar { Name = "Mega Jane" }
            };

            using (var store = GetDocumentStore())
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    await bulkInsert.StoreAsync(fooBars[0]);
                    await bulkInsert.StoreAsync(fooBars[1]);
                    await bulkInsert.StoreAsync(fooBars[2]);
                    await bulkInsert.StoreAsync(fooBars[3]);
                }

                using (var session = store.OpenSession())
                {
                    var doc1 = session.Load<FooBar>("FooBars/1-A");
                    var doc2 = session.Load<FooBar>("FooBars/2-A");
                    var doc3 = session.Load<FooBar>("FooBars/3-A");
                    var doc4 = session.Load<FooBar>("FooBars/4-A");

                    Assert.NotNull(doc1);
                    Assert.NotNull(doc2);
                    Assert.NotNull(doc3);
                    Assert.NotNull(doc4);

                    Assert.Equal("John Doe", doc1.Name);
                    Assert.Equal("Jane Doe", doc2.Name);
                    Assert.Equal("Mega John", doc3.Name);
                    Assert.Equal("Mega Jane", doc4.Name);
                }
            }
        }

        [RavenFact(RavenTestCategory.BulkInsert)]
        public async Task KilledTooEarly()
        {
            var exception = await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                using (var store = GetDocumentStore())
                {
                    using (var bulkInsert = store.BulkInsert())
                    {
                        await bulkInsert.StoreAsync(new FooBar());
                        await bulkInsert.AbortAsync();
                        await bulkInsert.StoreAsync(new FooBar());
                    }
                }
            });

            if (exception is AggregateException ae)
                exception = ae.ExtractSingleInnerException();

            Assert.True(exception is BulkInsertAbortedException);
        }

        [RavenFact(RavenTestCategory.BulkInsert)]
        public void ShouldNotAcceptIdsEndingWithPipeLine()
        {
            using (var store = GetDocumentStore())
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    var exception = Assert.Throws<NotSupportedException>(() =>
                        bulkInsert.Store(new FooBar { Name = "John Doe" }, "foobars|"));
                    Assert.Contains("Document ids cannot end with '|', but was called with foobars|", exception.Message);
                }
            }
        }

        [RavenFact(RavenTestCategory.BulkInsert)]
        public void CanModifyMetadataWithBulkInsert()
        {
            var expirationDate = DateTime.Today.AddYears(1).ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
            using (var store = GetDocumentStore())
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new FooBar
                    {
                        Name = "Jon Snow"
                    }, new MetadataAsDictionary
                    {
                        [Constants.Documents.Metadata.Expires] = expirationDate
                    });
                }

                using (var session = store.OpenSession())
                {
                    var entity = session.Load<FooBar>("FooBars/1-A");
                    var metadataExpirationDate = session.Advanced.GetMetadataFor(entity)[Constants.Documents.Metadata.Expires];

                    Assert.Equal(expirationDate, metadataExpirationDate);
                }
            }
        }

        [RavenFact(RavenTestCategory.BulkInsert)]
        public async Task BulkInsertDynamic_WhenProvideDelegateForDynamicCollectionAndType_ShouldUseIt()
        {
            const string customCollection = "CustomCollection";
            const string customType = "CustomType";
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.AddIdFieldToDynamicObjects = false;
                    s.Conventions.FindCollectionNameForDynamic = (entity) => customCollection;
                    s.Conventions.FindClrTypeNameForDynamic = (entity) => customType;
                }
            });

            var str = @"
{
    ""heading"": ""Hello, world ⭐️"",
}";
            var o = JsonConvert.DeserializeObject<ExpandoObject>(str, new ExpandoObjectConverter());
            await using (var bulkInsert = store.BulkInsert())
            {
                await bulkInsert.StoreAsync(o, "o/1");
            }
            using (var session = store.OpenAsyncSession())
            {
                var d = await session.LoadAsync<object>("o/1");
                IDictionary<string, object> metadata = session.Advanced.GetMetadataFor(d);
                Assert.Equal(customCollection, metadata["@collection"]);
                Assert.Equal(customType, metadata["Raven-Clr-Type"]);
            }
        }
        
        private class FooBar
        {
            public string Name { get; set; }
        }
    }
}
