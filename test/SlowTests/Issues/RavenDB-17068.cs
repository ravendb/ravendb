using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Collections;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_17068: RavenTestBase
{
    public RavenDB_17068(ITestOutputHelper output) : base(output)
    {
        
    }
    
    [RavenFact(RavenTestCategory.Indexes)]
    public async Task CheckIfMismatchesAreRemovedOnMatchingLoad()
    {
        using var store = GetDocumentStore();
        var db = await GetDatabase(store.Database);
        
        var notificationsQueue = new AsyncQueue<DynamicJsonValue>();

        using (db.NotificationCenter.TrackActions(notificationsQueue, null))
        using (var session = store.OpenSession())
        {
            var c1 = new Cat() { Name = "Bingus" };

            session.Store(c1);
            
            var a1 = new Animal() { Name = "CoolAnimal" };
            
            session.Store(a1);

            var o1 = new Order() { AnimalId = c1.Id, Price = 21 };
            var o2 = new Order() { AnimalId = a1.Id, Price = 37 };

            session.Store(o1);
            session.Store(o2);

            session.SaveChanges();

            var index = new DummyIndex();
            
            await index.ExecuteAsync(store);
            Indexes.WaitForIndexing(store);
            Tuple<bool, DynamicJsonValue> alertRaised;

            do
            {
                alertRaised = await notificationsQueue.TryDequeueAsync(TimeSpan.FromSeconds(5));
                
            } while (alertRaised.Item2["Type"].ToString() != NotificationType.AlertRaised.ToString());
            
            var details = alertRaised.Item2[nameof(AlertRaised.Details)] as DynamicJsonValue;
            
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var json = ctx.ReadObject(details, "foo");

                var detailsObject = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<MismatchedReferencesLoadWarning>(json, "Warnings");
                
                Assert.Equal("DummyIndex",detailsObject.IndexName);
                Assert.Equal(1, detailsObject.Warnings.Count);

                detailsObject.Warnings.TryGetValue("orders/2-A", out var warnings);
                
                Assert.NotNull(warnings);
                Assert.Equal(1, warnings.Count);
                Assert.Equal("animals/1-A", warnings.First().ReferenceId);
                Assert.Equal("orders/2-A", warnings.First().SourceId);
                Assert.Equal(2, warnings.First().MismatchedCollections.Count);
            }
        }
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public async Task CheckIfNotificationIsNotSendWithAllLoadsEventuallyMatching()
    {
        using var store = GetDocumentStore();
        var db = await GetDatabase(store.Database);
        
        var notificationsQueue = new AsyncQueue<DynamicJsonValue>();
        
        using (db.NotificationCenter.TrackActions(notificationsQueue, null))
        using (var session = store.OpenSession())
        {
            var d1 = new Dog() { Name = "CoolDog1" };
            var d2 = new Dog() { Name = "CoolDog2" };

            session.Store(d1);
            session.Store(d2);
            
            session.SaveChanges();

            var index = new DummyIndex();
            
            await index.ExecuteAsync(store);
            Indexes.WaitForIndexing(store);
            Tuple<bool, DynamicJsonValue> alertRaised = await notificationsQueue.TryDequeueAsync(TimeSpan.FromSeconds(5));
            
            while (alertRaised.Item1)
            {
                Assert.NotEqual(NotificationType.AlertRaised.ToString(), alertRaised.Item2["Type"].ToString());
                alertRaised = await notificationsQueue.TryDequeueAsync(TimeSpan.FromSeconds(5));
            }
        }
    }
    
    private class DummyIndex : AbstractIndexCreationTask<Order>
    {
        public DummyIndex()
        {
            Map = orders => from o in orders
                let a = LoadDocument<Animal>(o.AnimalId, "Cats") ?? LoadDocument<Animal>(o.AnimalId, "Dogs")
                let b = LoadDocument<Animal>(o.AnimalId, "Dogs") ?? LoadDocument<Animal>(o.AnimalId, "Cats")
                select new { Content = a.Name };
            StoreAllFields(FieldStorage.Yes);
        }
    }

    private class Order
    {
        public string Id { get; set; }
        public string AnimalId { get; set; }
        
        public int Price { get; set; }
    }
    
    private class Animal
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class Cat
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class Dog
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}
