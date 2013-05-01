using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client;
using Raven.Client.Indexes;

namespace Raven.Tryouts
{
	public class Order
	{
		public string Product { get; set; }
		public decimal Total { get; set; }
		public Currency Currency { get; set; }
	}

	public enum Currency
	{
		USD,
		EUR,
		NIS
	}

	class Program
	{
		static void Main(string[] args)
		{
			Console.WriteLine("Open Server and press any key");
			Console.ReadLine();
			//CreateData();

           // CreateIndex();

		    Test();

		}

	    private static void CreateIndex()
	    {
	        using (
	            var store =
	                new DocumentStore {Url = "http://localhost:8080", DefaultDatabase = "AggregateQuerySample"}.Initialize())
	        {
	            store.DatabaseCommands.PutIndex("Orders/All",
	                                            new IndexDefinitionBuilder<Order>
	                                            {
	                                                Map = orders => from order in orders
	                                                                select
	                                                                    new {order.Currency, order.Product, order.Total},
	                                                SortOptions = {{x => x.Total, SortOptions.Double}}
	                                            }, true);
	        }
	    }

	    private static void Test()
		{
			using (var store = new DocumentStore {Url = "http://localhost:8080", DefaultDatabase = "AggregateQuerySample"}.Initialize())
			using(var session = store.OpenSession())
			{
				var sw = new Stopwatch();

				sw.Start();
				var r = session.Query<Order>("Orders/All")
                        .Where(x=>x.Total > 750)
					   .AggregateBy(order => order.Product)
						  .SumOn(order => order.Total)
					   .AndAggregateOn(order => order.Currency)
						   .SumOn(order => order.Total)
					   .ToList();


			    Output(r);


				Console.WriteLine("test 1 took {0:#,#} ms", sw.ElapsedMilliseconds);
				sw.Restart();
				r = session.Query<Order>("Orders/All")
                    .Where(x => x.Total > 750)
				       .AggregateBy(x => x.Product)
						 .SumOn(x => x.Total)
				       .AndAggregateOn(x => x.Total)
						   .AddRanges(x => x.Total < 100,
									  x => x.Total >= 100 && x.Total < 500,
									  x => x.Total >= 500 && x.Total < 1500,
									  x => x.Total >= 1500)
				       .SumOn(x => x.Total)
				       .ToList();
                Output(r);

				Console.WriteLine("test 2 took {0:#,#} ms", sw.ElapsedMilliseconds);
				sw.Restart();
				r = session.Query<Order>("Orders/All")
                    .Where(x => x.Total > 750)
				       .AggregateBy(x => x.Product)
						 .SumOn(x => x.Total)
				       .AndAggregateOn(x => x.Product)
					     .AverageOn(x => x.Total)
				       .ToList();
                Output(r);

				Console.WriteLine("test 3 took {0:#,#} ms", sw.ElapsedMilliseconds);
			}

		}

	    private static void Output(FacetResults r)
	    {
	        foreach (var facetResult in r.Results)
	        {
	            Console.WriteLine(facetResult.Key);
	            foreach (var v in facetResult.Value.Values)
	            {
	                Console.WriteLine("\t" + v.Range + ": " + v);
	            }
	        }
	    }

	    public static void CreateData()
		{
			using(var store = new DocumentStore{Url = "http://localhost:8080", DefaultDatabase = "AggregateQuerySample"}.Initialize())
			{
				var data = GenerateData();

			    using (var bulk = store.BulkInsert())
			    {

			        foreach (var order in data)
			        {
			            bulk.Store(order);
			        }
			    }
			}
		}

		private static IEnumerable<Order> GenerateData()
		{
			var random = new Random();
			var values = Enum.GetValues(typeof(Currency));
			var products = new List<string> {"RavenDB", "RavenFS", "UberProf", "NHibernate"};

			for (int i = 0; i < 1000000; i++)
			{
			    yield return (new Order
                 {
                     Currency = (Currency)values.GetValue(random.Next(values.Length)),
                     Product = products[random.Next(products.Count)],
                     Total = (decimal)Math.Round((random.NextDouble() * (1000 - 500) + 500), 2)
                 });
			}
		}


	}
}