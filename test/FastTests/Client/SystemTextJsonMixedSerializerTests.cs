using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Json.Serialization;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Raven.Client.Json.Serialization.SystemTextJson;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Client;

public class SystemTextJsonMixedSerializerTests(ITestOutputHelper output) : RavenTestBase(output)
{
    private static readonly TimeSpan WaitTime = TimeSpan.FromSeconds(60);

    public class Animal
    {
        public string Name { get; set; }
    }

    public class Dog : Animal
    {
        public bool Barks { get; set; }
    }

    public enum Status
    {
        Draft,

        [EnumMember(Value = "in-review")]
        InReview,

        Done
    }

    public class Location
    {
        public double Latitude { get; set; }

        public double Longitude { get; set; }
    }

    [DataContract]
    public class Address
    {
        [DataMember(Name = "city")]
        public string City { get; set; }

        [DataMember]
        public Location Location { get; set; }

        public string NotStored { get; set; }
    }

    public class OrderLine
    {
        public string Product { get; set; }

        public int Quantity { get; set; }

        public decimal Price { get; set; }
    }

    public class Order
    {
        public string Id { get; set; }

        [JsonProperty("customer_name")]
        public string CustomerName { get; set; }

        public Status Status { get; set; }

        public DateTime CreatedAt { get; set; }

        public DateTimeOffset UpdatedAt { get; set; }

        public decimal Total { get; set; }

        public double Discount { get; set; }

        public Guid Reference { get; set; }

        public int? Priority { get; set; }

        public Address ShipTo { get; set; }

        public List<OrderLine> Lines { get; set; }

        public Dictionary<string, decimal> Taxes { get; set; }

        public Animal Pet { get; set; }

        public object Extra { get; set; }

        public List<string> Tags { get; } = new();

        private string InternalNote { get; set; }

        public string GetInternalNote() => InternalNote;

        public void SetInternalNote(string note) => InternalNote = note;
    }

    public class PartialLocation
    {
        public double Latitude { get; set; }
    }

    public class PartialAddress
    {
        public PartialLocation Location { get; set; }
    }

    public class PartialLine
    {
        public string Product { get; set; }
    }

    public class PartialOrder
    {
        public string Id { get; set; }

        [JsonProperty("customer_name")]
        public string CustomerName { get; set; }

        public PartialAddress ShipTo { get; set; }

        public List<PartialLine> Lines { get; set; }
    }

    public class OrderSummary
    {
        public string Customer { get; set; }

        public decimal Total { get; set; }
    }

    public class PetHolder
    {
        public Animal Pet { get; set; }

        public Status Status { get; set; }

        public DateTime When { get; set; }
    }

    [RavenTheory(RavenTestCategory.ClientApi)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LoadsDocumentsWrittenByTheOtherSerializerWithoutChanges(bool writerUsesSystemTextJson)
    {
        using var writer = GetDocumentStore(Pinned(writerUsesSystemTextJson));
        using var reader = OpenStore(writer, useSystemTextJson: writerUsesSystemTextJson == false);

        using (var session = writer.OpenAsyncSession())
        {
            await session.StoreAsync(CreateOrder("orders/1", "Oren", 10.50m));
            await session.StoreAsync(new Dog { Name = "Rex", Barks = true }, "dogs/1");
            await session.SaveChangesAsync();
        }

        string orderBefore = await GetRawAsync(writer, "orders/1");
        string dogBefore = await GetRawAsync(writer, "dogs/1");

        using (var session = reader.OpenAsyncSession())
        {
            Order order = await session.LoadAsync<Order>("orders/1");
            AssertOrder(CreateOrder("orders/1", "Oren", 10.50m), order);

            Animal dog = await session.LoadAsync<Animal>("dogs/1");
            Assert.True(Assert.IsType<Dog>(dog).Barks);

            Assert.False(session.Advanced.HasChanges);
            Assert.Empty(session.Advanced.WhatChanged());
            await session.SaveChangesAsync();
        }

        Assert.Equal(orderBefore, await GetRawAsync(writer, "orders/1"));
        Assert.Equal(dogBefore, await GetRawAsync(writer, "dogs/1"));
    }

    [RavenTheory(RavenTestCategory.ClientApi)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SavingAPartialModelKeepsFieldsWrittenByTheOtherSerializer(bool writerUsesSystemTextJson)
    {
        using var writer = GetDocumentStore(Pinned(writerUsesSystemTextJson));
        using var reader = OpenStore(writer, useSystemTextJson: writerUsesSystemTextJson == false);

        using (var session = writer.OpenAsyncSession())
        {
            await session.StoreAsync(CreateOrder("orders/1", "Oren", 10.50m));
            await session.SaveChangesAsync();
        }

        using (var session = reader.OpenAsyncSession())
        {
            PartialOrder partial = await session.LoadAsync<PartialOrder>("orders/1");
            Assert.False(session.Advanced.HasChanges);

            partial.CustomerName = "Ayende";
            partial.Lines[0].Product = "renamed";
            await session.SaveChangesAsync();
        }

        Order expected = CreateOrder("orders/1", "Ayende", 10.50m);
        expected.Lines[0].Product = "renamed";

        using (var session = writer.OpenAsyncSession())
            AssertOrder(expected, await session.LoadAsync<Order>("orders/1"));
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task QueriesReturnTheSameResultsForDocumentsWrittenByTheOtherSerializer(bool writerUsesSystemTextJson)
    {
        using var writer = GetDocumentStore(Pinned(writerUsesSystemTextJson));
        using var reader = OpenStore(writer, useSystemTextJson: writerUsesSystemTextJson == false);

        using (var session = writer.OpenAsyncSession())
        {
            await session.StoreAsync(CreateOrder("orders/1", "Oren", 10.50m));
            await session.StoreAsync(CreateOrder("orders/2", "Ayende", 20m, Status.Done));
            await session.StoreAsync(CreateOrder("orders/3", "Grisha", 30.25m, Status.Done));
            await session.SaveChangesAsync();
        }

        Assert.Equal(["orders/2"], await QueryIdsAsync(reader, q => q.Where(x => x.CustomerName == "Ayende")));
        Assert.Equal(["orders/2", "orders/3"], await QueryIdsAsync(reader, q => q.Where(x => x.Status == Status.Done).OrderBy(x => x.Id)));

        foreach (IDocumentStore store in new[] { writer, reader })
        {
            Assert.Equal(["orders/3", "orders/2"], await QueryIdsAsync(store, q => q.Where(x => x.Total > 15).OrderByDescending(x => x.Total)));
            Assert.Equal(["orders/1"], await QueryIdsAsync(store, q => q.Where(x => x.Priority == null)));
        }

        Assert.Equal(await ProjectAsync(writer), await ProjectAsync(reader));
        Assert.Contains("Grisha 30.25", await ProjectAsync(reader));
    }

    [RavenTheory(RavenTestCategory.Patching)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PatchesByOneSerializerLoadInTheOther(bool writerUsesSystemTextJson)
    {
        using var writer = GetDocumentStore(Pinned(writerUsesSystemTextJson));
        using var patcher = OpenStore(writer, useSystemTextJson: writerUsesSystemTextJson == false);

        using (var session = writer.OpenAsyncSession())
        {
            await session.StoreAsync(CreateOrder("orders/1", "Oren", 10.50m));
            await session.SaveChangesAsync();
        }

        using (var session = patcher.OpenAsyncSession())
        {
            session.Advanced.Patch<Order, Animal>("orders/1", x => x.Pet, new Dog { Name = "Patched", Barks = true });
            session.Advanced.Patch<Order, Status>("orders/1", x => x.Status, Status.Done);
            await session.SaveChangesAsync();
        }

        using (var session = writer.OpenAsyncSession())
        {
            Order order = await session.LoadAsync<Order>("orders/1");
            Assert.Equal("Patched", Assert.IsType<Dog>(order.Pet).Name);
            Assert.Equal(Status.Done, order.Status);
            Assert.False(session.Advanced.HasChanges);
        }
    }

    [RavenTheory(RavenTestCategory.CompareExchange)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CompareExchangeValuesWrittenByOneSerializerLoadInTheOther(bool writerUsesSystemTextJson)
    {
        using var writer = GetDocumentStore(Pinned(writerUsesSystemTextJson));
        using var reader = OpenStore(writer, useSystemTextJson: writerUsesSystemTextJson == false);

        var when = new DateTime(2026, 11, 2, 10, 30, 0, DateTimeKind.Utc);
        await writer.Operations.SendAsync(new PutCompareExchangeValueOperation<PetHolder>("pets/1", new PetHolder
        {
            Pet = new Dog { Name = "Rex", Barks = true },
            Status = Status.InReview,
            When = when
        }, 0));

        CompareExchangeValue<PetHolder> value = await reader.Operations.SendAsync(new GetCompareExchangeValueOperation<PetHolder>("pets/1"));

        Assert.True(Assert.IsType<Dog>(value.Value.Pet).Barks);
        Assert.Equal(Status.InReview, value.Value.Status);
        Assert.Equal(when, value.Value.When);
    }

    [RavenTheory(RavenTestCategory.Subscriptions)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SubscriptionsDeliverDocumentsWrittenByTheOtherSerializer(bool writerUsesSystemTextJson)
    {
        using var writer = GetDocumentStore(Pinned(writerUsesSystemTextJson));
        using var reader = OpenStore(writer, useSystemTextJson: writerUsesSystemTextJson == false);

        using (var session = writer.OpenAsyncSession())
        {
            await session.StoreAsync(CreateOrder("orders/1", "Oren", 10.50m));
            await session.StoreAsync(CreateOrder("orders/2", "Ayende", 20m, Status.Done));
            await session.SaveChangesAsync();
        }

        string name = await reader.Subscriptions.CreateAsync<Order>();
        using (SubscriptionWorker<Order> worker = reader.Subscriptions.GetSubscriptionWorker<Order>(new SubscriptionWorkerOptions(name)
        {
            TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
        }))
        {
            var orders = new BlockingCollection<Order>();
            _ = worker.Run(batch => batch.Items.ForEach(x => orders.Add(x.Result)));

            var received = new Dictionary<string, Order>();
            for (int i = 0; i < 2; i++)
            {
                Assert.True(orders.TryTake(out Order order, WaitTime));
                received[order.Id] = order;
            }

            AssertOrder(CreateOrder("orders/1", "Oren", 10.50m), received["orders/1"]);
            AssertOrder(CreateOrder("orders/2", "Ayende", 20m, Status.Done), received["orders/2"]);
        }
    }

    private static Options Pinned(bool useSystemTextJson) => new()
    {
        ModifyDocumentStore = s => s.Conventions.Serialization = CreateSerializationConventions(useSystemTextJson)
    };

    private static ISerializationConventions CreateSerializationConventions(bool useSystemTextJson) =>
        useSystemTextJson ? new SystemTextJsonSerializationConventions() : new NewtonsoftJsonSerializationConventions();

    private static IDocumentStore OpenStore(IDocumentStore source, bool useSystemTextJson)
    {
        var store = new DocumentStore
        {
            Urls = source.Urls,
            Database = source.Database,
            Certificate = source.Certificate
        };

        store.Conventions.Serialization = CreateSerializationConventions(useSystemTextJson);
        return store.Initialize();
    }

    private static async Task<string> GetRawAsync(IDocumentStore store, string id)
    {
        using (var session = store.OpenAsyncSession())
            return (await session.LoadAsync<BlittableJsonReaderObject>(id)).ToString();
    }

    private static async Task<string[]> QueryIdsAsync(IDocumentStore store, Func<IQueryable<Order>, IQueryable<Order>> query)
    {
        using (var session = store.OpenAsyncSession())
        {
            List<Order> orders = await query(session.Query<Order>().Customize(x => x.WaitForNonStaleResults())).ToListAsync();
            return orders.Select(x => x.Id).ToArray();
        }
    }

    private static async Task<string[]> ProjectAsync(IDocumentStore store)
    {
        using (var session = store.OpenAsyncSession())
        {
            List<OrderSummary> summaries = await session.Query<Order>()
                .Customize(x => x.WaitForNonStaleResults())
                .OrderBy(x => x.Total)
                .Select(x => new OrderSummary { Customer = x.CustomerName, Total = x.Total })
                .ToListAsync();

            return summaries.Select(x => FormattableString.Invariant($"{x.Customer} {x.Total}")).ToArray();
        }
    }

    private static Order CreateOrder(string id, string customer, decimal total, Status status = Status.InReview)
    {
        var order = new Order
        {
            Id = id,
            CustomerName = customer,
            Status = status,
            CreatedAt = new DateTime(2026, 11, 2, 10, 30, 45, 123, DateTimeKind.Utc),
            UpdatedAt = new DateTimeOffset(2026, 11, 2, 12, 30, 45, TimeSpan.FromHours(2)),
            Total = total,
            Discount = 0.1,
            Reference = Guid.Parse("0f8fad5b-d9cb-469f-a165-70867728950e"),
            Priority = status == Status.Done ? 3 : null,
            ShipTo = new Address
            {
                City = "Hadera",
                Location = new Location { Latitude = 32.4340458, Longitude = 34.9196518 },
                NotStored = "not stored"
            },
            Lines =
            [
                new OrderLine { Product = "products/1", Quantity = 2, Price = 1.50m },
                new OrderLine { Product = "products/2", Quantity = 1, Price = 7.50m }
            ],
            Taxes = new Dictionary<string, decimal> { ["vat"] = 0.17m },
            Pet = new Dog { Name = "Rex", Barks = true },
            Extra = new Dog { Name = "Extra", Barks = false }
        };

        order.Tags.Add("tag");
        order.SetInternalNote("internal");
        return order;
    }

    private static void AssertOrder(Order expected, Order actual)
    {
        Assert.Equal(expected.Id, actual.Id);
        Assert.Equal(expected.CustomerName, actual.CustomerName);
        Assert.Equal(expected.Status, actual.Status);
        Assert.Equal(expected.CreatedAt, actual.CreatedAt);
        Assert.Equal(expected.UpdatedAt, actual.UpdatedAt);
        Assert.Equal(expected.Total, actual.Total);
        Assert.Equal(expected.Discount, actual.Discount);
        Assert.Equal(expected.Reference, actual.Reference);
        Assert.Equal(expected.Priority, actual.Priority);
        Assert.Equal(expected.ShipTo.City, actual.ShipTo.City);
        Assert.Equal(expected.ShipTo.Location.Latitude, actual.ShipTo.Location.Latitude);
        Assert.Equal(expected.ShipTo.Location.Longitude, actual.ShipTo.Location.Longitude);
        Assert.Null(actual.ShipTo.NotStored);
        Assert.Equal(expected.Lines.Select(x => (x.Product, x.Quantity, x.Price)), actual.Lines.Select(x => (x.Product, x.Quantity, x.Price)));
        Assert.Equal(expected.Taxes, actual.Taxes);
        Assert.Equal(expected.Pet.Name, Assert.IsType<Dog>(actual.Pet).Name);
        Assert.Equal(((Dog)expected.Extra).Name, Assert.IsType<Dog>(actual.Extra).Name);
        Assert.Equal(expected.Tags, actual.Tags);
        Assert.Equal(expected.GetInternalNote(), actual.GetInternalNote());
    }
}
