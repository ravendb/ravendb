using FizzWare.NBuilder.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDb_2215 : ReplicationBase 
    {
            public class Sale
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

                    Sort(x => x.Nett, SortOptions.Double);
                    Sort(x => x.Val, SortOptions.Int);
                    Store(x => x.Nett, FieldStorage.Yes);
                }
            }
           



            [Fact]
            public void QueryReturningMultipleValues()
            {

                using (var store = CreateEmbeddableStore()) 
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
                           .AggregateBy(x => x.IsCancelled)
                                .SumOn(x => x.Nett)
                                .CountOn(x => x.Id)
                           .ToList();

                        double cancelledFinanceSum = 0;
                        double cancelledFinanceCount = 0;
                        if (failedFinance.Results["IsCancelled"].Values.Any())
                        {
                            cancelledFinanceSum = failedFinance.Results["IsCancelled"].Values[0].Sum.GetValueOrDefault(0);
                            cancelledFinanceCount = failedFinance.Results["IsCancelled"].Values[0].Count.GetValueOrDefault(0);
                        }

                        Assert.Equal(5, cancelledFinanceCount);
                        Assert.Equal(6310, cancelledFinanceSum);
                    }
                }

            }


            [Fact]
            public void QueryReturningMultipleValuesWithDifferentNames()
            {

                using (var store = CreateEmbeddableStore())
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
                           .AggregateBy(x => x.IsCancelled)
                                .SumOn(x => x.Nett)
                           .AndAggregateOn(x => x.IsCancelled, "AndAggregateOnName")
                                .AverageOn(x => x.Val)
                           .ToList();

                        double cancelledFinanceSum = 0;
                        double cancelledFinanceAverage = 0;
                        if (failedFinance.Results["IsCancelled"].Values.Any())
                        {
                            Assert.Equal(2, failedFinance.Results.Count);
                            cancelledFinanceSum = failedFinance.Results["IsCancelled"].Values[0].Sum.GetValueOrDefault(0);
                            cancelledFinanceAverage = failedFinance.Results["AndAggregateOnName"].Values[0].Average.GetValueOrDefault(0);
                        }

                        Assert.Equal(4, cancelledFinanceAverage);
                        Assert.Equal(6310, cancelledFinanceSum);
                    }
                }

            }
            [Fact]
            public void QueryCantReturnMultipleAggregationValuesWithSameName()
            {

                using (var store = CreateEmbeddableStore())
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
                        var ex = Assert.Throws<InvalidOperationException>(() =>
                            session.Query<SalesIndex.Result, SalesIndex>()
                                .Where(x => x.IsCancelled)
                                .AggregateBy(x => x.IsCancelled)
                                .SumOn(x => x.Nett)
                                .AndAggregateOn(x => x.IsCancelled)
                                .AverageOn(x => x.Val)
                                .ToList());
                    }
                    using (var session = store.OpenSession())
                    {
                        var ex = Assert.Throws<InvalidOperationException>(() =>
                            session.Query<SalesIndex.Result, SalesIndex>()
                                .Where(x => x.IsCancelled)
                                .AggregateBy(x => x.IsCancelled,"Name1")
                                .SumOn(x => x.Nett)
                                .AndAggregateOn(x => x.IsCancelled, "Name1")
                                .AverageOn(x => x.Val)
                                .ToList());
                    }

                    
                }

            }
            [Fact]
            public void CantQueryReturningMultipleValuesOnDifferentArguments()
            {

                using (var store = CreateEmbeddableStore())
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
                         var ex = Assert.Throws<InvalidOperationException>(() => session.Query<SalesIndex.Result, SalesIndex>()
                           .Where(x => x.IsCancelled)
                           .AggregateBy(x => x.IsCancelled)
                                .SumOn(x => x.Nett)
                                .AverageOn(x => x.Val)// should throw, invalid
                           .ToList());
                    }
                }

            }
            [Fact]
            public void QueryReturningMultipleValuesSameArg()
            {

                using (var store = CreateEmbeddableStore())
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
                           .AggregateBy(x => x.IsCancelled)
                                .SumOn(x => x.Nett)
                                .AverageOn(x => x.Nett)
                           .ToList();
                       
                        double cancelledFinanceSum = 0;
                        double cancelledFinanceAverage = 0;
                        if (failedFinance.Results["IsCancelled"].Values.Any())
                        {
                            Assert.Equal(1, failedFinance.Results.Count);
                            cancelledFinanceSum = failedFinance.Results["IsCancelled"].Values[0].Sum.GetValueOrDefault(0);
                            cancelledFinanceAverage = failedFinance.Results["IsCancelled"].Values[0].Average.GetValueOrDefault(0);
                        }
                        Assert.Equal(1262, cancelledFinanceAverage);
                        Assert.Equal(6310, cancelledFinanceSum);
  
                    }
                }

            }
        }
    
}
