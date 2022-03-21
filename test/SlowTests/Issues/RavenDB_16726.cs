using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Collections;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16726 : RavenTestBase
    {
        public RavenDB_16726(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Should_create_performance_hint_notification_when_exceeding_max_number_of_LoadDocument_calls_per_reference()
        {
            using (var store = GetDocumentStore())
            {
                var db = await GetDatabase(store.Database);
                db.Configuration.PerformanceHints.MaxNumberOfLoadsPerReference = 10;

                db.NotificationCenter.Indexing.MinUpdateInterval = TimeSpan.MinValue;

                var index = new Products_ByCategory();

                using (var session = store.OpenSession())
                {
                    session.Store(new Category { Id = "categories/0", Name = "foo"});
                    session.Store(new Category { Id = "categories/1", Name = "bar"});

                    for (int i = 0; i < 200; i++)
                    {
                        session.Store(new Product { Category = $"categories/{i % 2}"});
                    }

                    session.SaveChanges();
                }

                await index.ExecuteAsync(store);

                Indexes.WaitForIndexing(store);

                var notificationsQueue = new AsyncQueue<DynamicJsonValue>();

                using (db.NotificationCenter.TrackActions(notificationsQueue, null))
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new Category { Id = "categories/0", Name = "abc" });
                        
                        session.SaveChanges();
                    }

                    Indexes.WaitForIndexing(store);

                    Tuple<bool, DynamicJsonValue> performanceHint;

                    do
                    {
                        performanceHint = await notificationsQueue.TryDequeueAsync(TimeSpan.FromSeconds(5));
                    } while (performanceHint.Item2["Type"].ToString() != NotificationType.PerformanceHint.ToString());

                    Assert.NotNull(performanceHint.Item2);

                    Assert.Equal("We have detected high number of LoadDocument() / LoadCompareExchangeValue() calls per single reference item. The update of a reference will result in reindexing all documents that reference it. Please see Indexing Performance graph to check the performance of your indexes.",
                        performanceHint.Item2[nameof(PerformanceHint.Message)]);

                    Assert.Equal(PerformanceHintType.Indexing_References, performanceHint.Item2[nameof(PerformanceHint.HintType)]);

                    var details = performanceHint.Item2[nameof(PerformanceHint.Details)] as DynamicJsonValue;

                    Assert.NotNull(details);

                    using (var ctx = JsonOperationContext.ShortTermSingleUse())
                    {
                        var json = ctx.ReadObject(details, "foo");

                        var detailsObject = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<IndexingReferenceLoadWarning>(json, "bar");

                        Assert.Contains("Products/ByCategory", detailsObject.Warnings.Keys);
                        Assert.Equal(1, detailsObject.Warnings.Count);

                        var top10LoadedReferences = detailsObject.Warnings["Products/ByCategory"].Top10LoadedReferences;

                        Assert.Equal(1, top10LoadedReferences.Count);
                        Assert.Equal("categories/0", top10LoadedReferences["categories/0"].ReferenceId);
                        Assert.Equal(100, top10LoadedReferences["categories/0"].NumberOfLoads);
                    }

                    // update of the hint

                    using (var session = store.OpenSession())
                    {
                        session.Store(new Category { Id = "categories/1", Name = "def" });

                        session.SaveChanges();
                    }

                    Indexes.WaitForIndexing(store);

                    db.NotificationCenter.Indexing.UpdateIndexing(null);

                    do
                    {
                        performanceHint = await notificationsQueue.TryDequeueAsync(TimeSpan.FromSeconds(5));
                    } while (performanceHint.Item2["Type"].ToString() != NotificationType.PerformanceHint.ToString());

                    details = performanceHint.Item2[nameof(PerformanceHint.Details)] as DynamicJsonValue;

                    Assert.NotNull(details);

                    using (var ctx = JsonOperationContext.ShortTermSingleUse())
                    {
                        var json = ctx.ReadObject(details, "foo");

                        var detailsObject = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<IndexingReferenceLoadWarning>(json, "bar");


                        Assert.Contains("Products/ByCategory", detailsObject.Warnings.Keys);
                        Assert.Equal(1, detailsObject.Warnings.Count);

                        var top10LoadedReferences = detailsObject.Warnings["Products/ByCategory"].Top10LoadedReferences;

                        Assert.Equal(2, top10LoadedReferences.Count);

                        Assert.Equal("categories/0", top10LoadedReferences["categories/0"].ReferenceId);
                        Assert.Equal(100, top10LoadedReferences["categories/0"].NumberOfLoads);


                        Assert.Equal("categories/1", top10LoadedReferences["categories/1"].ReferenceId);
                        Assert.Equal(100, top10LoadedReferences["categories/1"].NumberOfLoads);
                    }
                }
            }
        }

        [Fact]
        public void Can_add_and_update_reference_load_warning_details()
        {
            var warningDetails = new IndexingReferenceLoadWarning.WarningDetails();

            for (int i = 0; i < IndexingReferenceLoadWarning.MaxNumberOfDetailsPerIndex + 4; i++)
            {
                var added = warningDetails.Add(new IndexingReferenceLoadWarning.LoadedReference { NumberOfLoads = i + 1, ReferenceId = $"categories/{i}" });

                Assert.True(added);
            }

            Assert.Equal(IndexingReferenceLoadWarning.MaxNumberOfDetailsPerIndex, warningDetails.Top10LoadedReferences.Count);

            // update item by greater NumberOfLoads
            Assert.True(warningDetails.Add(new IndexingReferenceLoadWarning.LoadedReference { NumberOfLoads = 1000, ReferenceId = "categories/5" }));
            Assert.Equal(IndexingReferenceLoadWarning.MaxNumberOfDetailsPerIndex, warningDetails.Top10LoadedReferences.Count);
            Assert.Equal(1000, warningDetails.Top10LoadedReferences["categories/5"].NumberOfLoads);

            // update item by lower NumberOfLoads
            Assert.True(warningDetails.Add(new IndexingReferenceLoadWarning.LoadedReference { NumberOfLoads = 100, ReferenceId = "categories/5" }));
            Assert.Equal(IndexingReferenceLoadWarning.MaxNumberOfDetailsPerIndex, warningDetails.Top10LoadedReferences.Count);
            Assert.Equal(100, warningDetails.Top10LoadedReferences["categories/5"].NumberOfLoads);
        }

        private class Products_ByCategory : AbstractIndexCreationTask<Product>
        {
            public Products_ByCategory()
            {
                Map = products => from product in products
                    let category = LoadDocument<Category>(product.Category)
                    select new
                    {
                        CategoryId = category.Name
                    };
            }
        }
    }
}
