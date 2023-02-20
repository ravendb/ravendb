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
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [InlineData(@"from index 'DummyIndex' as o")]
    public async Task Temp(string q)
    {
        using var store = GetDocumentStore();
        var db = await GetDatabase(store.Database);
        
        var notificationsQueue = new AsyncQueue<DynamicJsonValue>();
        using (db.NotificationCenter.TrackActions(notificationsQueue, null))
        using (var session = store.OpenSession())
        {
            var c1 = new Cat() { Name = "Bingus" };
            var c2 = new Cat() { Name = "Kitty" };
            var c3 = new Cat() { Name = "Jinx" };
            var c4 = new Cat() { Name = "Cat" };

            session.Store(c1);
            session.Store(c2);
            session.Store(c3);
            session.Store(c4);
            
            var d1 = new Dog() { Name = "Doggo" };
            
            session.Store(d1);

            var o1 = new Order() { AnimalId = c1.Id, AnimalId2 = c2.Id, AnimalId3 = c3.Id, AnimalId4 = c4.Id, Price = 21 };
            var o2 = new Order() { AnimalId = c2.Id, Price = 37 };
            var o3 = new Order() { AnimalId = c3.Id, Price = 11 };
            var o4 = new Order() { AnimalId = c4.Id, Price = 22 };
            
            session.Store(o1);
            session.Store(o2);
            session.Store(o3);
            session.Store(o4);
            
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
            }
        }
    }
    
    private class DummyIndex : AbstractIndexCreationTask<Order>
    {
        public DummyIndex()
        {
            Map = orders => from o in orders
                let a = LoadDocument<Animal>(o.AnimalId, "Cats") ?? LoadDocument<Animal>(o.AnimalId, "Dogs")
                let nothing = LoadDocument<object>(o.AnimalId, "NotAnAnimal") ?? LoadDocument<object>(o.AnimalId, "zzz")
                let nothing2 = LoadDocument<object>(o.AnimalId2, "NotAnAnimal2")
                let nothing3 = LoadDocument<object>(o.AnimalId3, "NotAnAnimal3")
                let nothing4 = LoadDocument<object>(o.AnimalId4, "NotAnAnimal4")
                select new { Content = a.Name };
            StoreAllFields(FieldStorage.Yes);
        }
    }

    private class Order
    {
        public string Id { get; set; }
        public string AnimalId { get; set; }
        
        public string AnimalId2 { get; set; }
        
        public string AnimalId3 { get; set; }
        
        public string AnimalId4 { get; set; }
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
