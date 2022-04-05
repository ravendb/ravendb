using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_18404 : RavenTestBase
{
    public RavenDB_18404(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void NullableDateOnlyWithMapReduce()
    {
        using var store = GetDocumentStore();
        {
            using var s = store.OpenSession();
            s.Store(new BussinessCustomer
            {
                Id = "bus1",
              
            });
            s.Store(new IndividualCustomer
            {
                Id = "cus1",
                BirthDate = new DateOnly(2022, 9, 10)
            });
            s.Store(new Contract
            {
                Id = "con1",
               
            });
            s.SaveChanges();
        }

        var index = new Core_Customers_CustomerProfile();
        index.Execute(store);
        Indexes.WaitForIndexing(store);
        WaitForUserToContinueTheTest(store);
        var indexErrors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { index.IndexName }));
        Assert.Equal(0, indexErrors[0].Errors.Length);
    }
    
    record BussinessCustomer
    {
        public string Id { get; init; }
    }

    record IndividualCustomer
    {
        public string Id { get; init; }
        public DateOnly BirthDate { get; init; }
    }

    record Contract
    {
        public string Id { get; init; }
    }

    public record CustomerProfileResult
    {
        public string Id { get; init; }
    }

    public record IndividualCustomerProfileResult : CustomerProfileResult
    {
        public DateOnly BirthDate { get; init; }
    }

    public record Result : IndividualCustomerProfileResult
    {
        public new DateOnly? BirthDate { get; init; }
    }
    
    private class Core_Customers_CustomerProfile : AbstractMultiMapIndexCreationTask<Result>
    {
        public Core_Customers_CustomerProfile()
        {
            AddMap<IndividualCustomer>(customers =>
                from customer in customers
                select new Result {Id = customer.Id, BirthDate = customer.BirthDate,});
            Reduce = results =>
                from result in results
                group result by result.Id
                into g
                select new Result {Id = g.Key, BirthDate = g.Select(x => x.BirthDate).First(x => x != null),};
        }
    }
}
