using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDb_2215 : RavenTestBase
    {
        private class Sale
        {
            public string Id { get; set; }
            public double Nett { get; set; }
            public double Val { get; set; }

            public bool IsCancelled { get; set; }
        }

        private class SalesIndex : AbstractIndexCreationTask<Sale, SalesIndex.Result>
        {
            public class Result
            {
                public string Id { get; set; }
                public double Nett { get; set; }
                public int Val { get; set; }

                public bool IsCancelled { get; set; }
            }

            public SalesIndex()
            {
                Map = sales => from sale in sales
                               select new
                               {
                                   Id = sale.Id,
                                   sale.IsCancelled,
                                   sale.Nett,
                                   sale.Val,
                               };

                Store(x => x.Nett, FieldStorage.Yes);
            }
        }

        [Fact]
        public void QueryReturningMultipleValues()
        {

            using (var store = GetDocumentStore())
            {
                new SalesIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Sale() { Id = "sales/1", IsCancelled = true, Nett = 1000, Val = 1 });
                    session.Store(new Sale() { Id = "sales/2", IsCancelled = true, Nett = 5000, Val = 2 });
                    session.Store(new Sale() { Id = "sales/3", IsCancelled = false, Nett = 10000, Val = 3 });
                    session.Store(new Sale() { Id = "sales/4", IsCancelled = true, Nett = 200, Val = 4 });
                    session.Store(new Sale() { Id = "sales/5", IsCancelled = false, Nett = 25, Val = 5 });
                    session.Store(new Sale() { Id = "sales/6", IsCancelled = true, Nett = 100, Val = 6 });
                    session.Store(new Sale() { Id = "sales/7", IsCancelled = true, Nett = 10, Val = 7 });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var failedFinance = session.Query<SalesIndex.Result, SalesIndex>()
                        .Where(x => x.IsCancelled)
                        .AggregateBy(
                            factory => factory
                                .ByField(x => x.IsCancelled)
                                .SumOn(x => x.Nett))
                        .Execute();

                    double cancelledFinanceSum = 0;
                    double cancelledFinanceCount = 0;
                    if (failedFinance["IsCancelled"].Values.Any())
                    {
                        cancelledFinanceSum = failedFinance["IsCancelled"].Values[0].Sum.GetValueOrDefault(0);
                        cancelledFinanceCount = failedFinance["IsCancelled"].Values[0].Count;
                    }

                    Assert.Equal(5, cancelledFinanceCount);
                    Assert.Equal(6310, cancelledFinanceSum);
                }
            }

        }


        [Fact]
        public void QueryReturningMultipleValuesWithDifferentNames()
        {
            using (var store = GetDocumentStore())
            {
                new SalesIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Sale() { Id = "sales/1", IsCancelled = true, Nett = 1000, Val = 1 });
                    session.Store(new Sale() { Id = "sales/2", IsCancelled = true, Nett = 5000, Val = 2 });
                    session.Store(new Sale() { Id = "sales/3", IsCancelled = false, Nett = 10000, Val = 3 });
                    session.Store(new Sale() { Id = "sales/4", IsCancelled = true, Nett = 200, Val = 4 });
                    session.Store(new Sale() { Id = "sales/5", IsCancelled = false, Nett = 25, Val = 5 });
                    session.Store(new Sale() { Id = "sales/6", IsCancelled = true, Nett = 100, Val = 6 });
                    session.Store(new Sale() { Id = "sales/7", IsCancelled = true, Nett = 10, Val = 7 });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var failedFinance = session.Query<SalesIndex.Result, SalesIndex>()
                        .Where(x => x.IsCancelled)
                        .AggregateBy(factory => factory.ByField(x => x.IsCancelled).SumOn(x => x.Nett))
                        .AndAggregateBy(
                            factory => factory
                                .ByField(x => x.IsCancelled)
                                .WithDisplayName("AndAggregateOnName")
                                .AverageOn(x => x.Val))
                        .Execute();

                    double cancelledFinanceSum = 0;
                    double cancelledFinanceAverage = 0;
                    if (failedFinance["IsCancelled"].Values.Any())
                    {
                        Assert.Equal(2, failedFinance.Count);
                        cancelledFinanceSum = failedFinance["IsCancelled"].Values[0].Sum.GetValueOrDefault(0);
                        cancelledFinanceAverage = failedFinance["AndAggregateOnName"].Values[0].Average.GetValueOrDefault(0);
                    }

                    Assert.Equal(4, cancelledFinanceAverage);
                    Assert.Equal(6310, cancelledFinanceSum);
                }
            }

        }
        [Fact]
        public void QueryCantReturnMultipleAggregationValuesWithSameName()
        {
            using (var store = GetDocumentStore())
            {
                new SalesIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Sale() { Id = "sales/1", IsCancelled = true, Nett = 1000, Val = 1 });
                    session.Store(new Sale() { Id = "sales/2", IsCancelled = true, Nett = 5000, Val = 2 });
                    session.Store(new Sale() { Id = "sales/3", IsCancelled = false, Nett = 10000, Val = 3 });
                    session.Store(new Sale() { Id = "sales/4", IsCancelled = true, Nett = 200, Val = 4 });
                    session.Store(new Sale() { Id = "sales/5", IsCancelled = false, Nett = 25, Val = 5 });
                    session.Store(new Sale() { Id = "sales/6", IsCancelled = true, Nett = 100, Val = 6 });
                    session.Store(new Sale() { Id = "sales/7", IsCancelled = true, Nett = 10, Val = 7 });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var ex = Assert.Throws<InvalidQueryException>(() =>
                        session.Query<SalesIndex.Result, SalesIndex>()
                            .Where(x => x.IsCancelled)
                            .AggregateBy(factory => factory.ByField(x => x.IsCancelled).SumOn(x => x.Nett))
                            .AndAggregateBy(factory => factory.ByField(x => x.IsCancelled).AverageOn(x => x.Val))
                            .Execute());

                    Assert.Contains("Duplicate alias 'IsCancelled' detected", ex.Message);
                }

                using (var session = store.OpenSession())
                {
                    var ex = Assert.Throws<InvalidQueryException>(() =>
                        session.Query<SalesIndex.Result, SalesIndex>()
                            .Where(x => x.IsCancelled)
                            .AggregateBy(factory => factory.ByField(x => x.IsCancelled).WithDisplayName("Name1").SumOn(x => x.Nett))
                            .AndAggregateBy(factory => factory.ByField(x => x.IsCancelled).WithDisplayName("Name1").AverageOn(x => x.Val))
                            .Execute());

                    Assert.Contains("Duplicate alias 'Name1' detected", ex.Message);
                }
            }
        }

        [Fact]
        public void CanQueryReturningMultipleValuesOnDifferentArguments()
        {
            using (var store = GetDocumentStore())
            {
                new SalesIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Sale() { Id = "sales/1", IsCancelled = true, Nett = 1000, Val = 1 });
                    session.Store(new Sale() { Id = "sales/2", IsCancelled = true, Nett = 5000, Val = 2 });
                    session.Store(new Sale() { Id = "sales/3", IsCancelled = false, Nett = 10000, Val = 3 });
                    session.Store(new Sale() { Id = "sales/4", IsCancelled = true, Nett = 200, Val = 4 });
                    session.Store(new Sale() { Id = "sales/5", IsCancelled = false, Nett = 25, Val = 5 });
                    session.Store(new Sale() { Id = "sales/6", IsCancelled = true, Nett = 100, Val = 6 });
                    session.Store(new Sale() { Id = "sales/7", IsCancelled = true, Nett = 10, Val = 7 });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<SalesIndex.Result, SalesIndex>()
                        .Where(x => x.IsCancelled)
                        .AggregateBy(f => f.ByField(x => x.IsCancelled).SumOn(x => x.Nett).AverageOn(x => x.Val))
                        .Execute();

                    Assert.Equal(6310, results["IsCancelled"].Values[0].Sum);
                    Assert.Equal(4, results["IsCancelled"].Values[0].Average);
                }
            }

        }
        [Fact]
        public void QueryReturningMultipleValuesSameArg()
        {
            using (var store = GetDocumentStore())
            {
                new SalesIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Sale() { Id = "sales/1", IsCancelled = true, Nett = 1000, Val = 1 });
                    session.Store(new Sale() { Id = "sales/2", IsCancelled = true, Nett = 5000, Val = 2 });
                    session.Store(new Sale() { Id = "sales/3", IsCancelled = false, Nett = 10000, Val = 3 });
                    session.Store(new Sale() { Id = "sales/4", IsCancelled = true, Nett = 200, Val = 4 });
                    session.Store(new Sale() { Id = "sales/5", IsCancelled = false, Nett = 25, Val = 5 });
                    session.Store(new Sale() { Id = "sales/6", IsCancelled = true, Nett = 100, Val = 6 });
                    session.Store(new Sale() { Id = "sales/7", IsCancelled = true, Nett = 10, Val = 7 });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var failedFinance = session.Query<SalesIndex.Result, SalesIndex>()
                       .Where(x => x.IsCancelled)
                       .AggregateBy(f => f.ByField(x => x.IsCancelled).SumOn(x => x.Nett).AverageOn(x => x.Nett))
                       .Execute();

                    double cancelledFinanceSum = 0;
                    double cancelledFinanceAverage = 0;
                    if (failedFinance["IsCancelled"].Values.Any())
                    {
                        Assert.Equal(1, failedFinance.Count);
                        cancelledFinanceSum = failedFinance["IsCancelled"].Values[0].Sum.GetValueOrDefault(0);
                        cancelledFinanceAverage = failedFinance["IsCancelled"].Values[0].Average.GetValueOrDefault(0);
                    }
                    Assert.Equal(1262, cancelledFinanceAverage);
                    Assert.Equal(6310, cancelledFinanceSum);

                }
            }

        }
    }

}
