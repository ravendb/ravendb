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

                public bool IsCancelled { get; set; }
            }

            private class SalesIndex : AbstractIndexCreationTask<Sale, SalesIndex.Result>
            {
                public class Result
                {
                    public string Id { get; set; }
                    public double Nett { get; set; }

                    public bool IsCancelled { get; set; }
                }

                public SalesIndex()
                {
                    Map = sales => from sale in sales
                                   select new
                                   {
                                       Id = sale.Id,
                                       sale.IsCancelled,
                                       sale.Nett
                                   };

                    Sort(x => x.Nett, SortOptions.Double);
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
                        session.Store(new Sale() { Id = "sales/1", IsCancelled = true, Nett = 1000 });
                        session.Store(new Sale() { Id = "sales/2", IsCancelled = true, Nett = 5000 });
                        session.Store(new Sale() { Id = "sales/3", IsCancelled = false, Nett = 10000 });
                        session.Store(new Sale() { Id = "sales/4", IsCancelled = true, Nett = 200 });
                        session.Store(new Sale() { Id = "sales/5", IsCancelled = false, Nett = 25 });
                        session.Store(new Sale() { Id = "sales/6", IsCancelled = true, Nett = 100 });
                        session.Store(new Sale() { Id = "sales/7", IsCancelled = true, Nett = 10 });
                        session.SaveChanges();
                    }
                 
                    WaitForIndexing(store);
                    using (var session = store.OpenSession())
                    {                        
                        var failedFinance = session.Query<SalesIndex.Result, SalesIndex>()
                            .Customize(x => x.WaitForNonStaleResults())
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

        }
    
}
