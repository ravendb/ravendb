namespace Raven.Tryouts
{

	using System;
	using System.Collections.Generic;
	using System.Linq;
	using Raven.Client.Document;
	using Raven.Client.Extensions;
	using Raven.Client.Indexes;

	public class Program
	{
		public class Application
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public string Author { get; set; }
			public Performance Performance { get; set; }
		}

		public class Performance
		{
			public List<Counter> Counters { get; set; }
		}

		public class Counter
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public decimal Value { get; set; }
			public DateTime DateTime { get; set; }
		}

		public class ApplicationByAuthor : AbstractIndexCreationTask<Application>
		{
			public ApplicationByAuthor()
			{
				Map = applications => from application in applications
									  select new
									  {
										  application.Author
									  };
			}
		}

		public class ApplicationByName : AbstractIndexCreationTask<Application>
		{
			public ApplicationByName()
			{
				Map = applications => from application in applications
									  select new
									  {
										  application.Name
									  };
			}
		}

		public class Counters_Sum : AbstractIndexCreationTask<Application, Counters_Sum.Result>
		{
			public class Result
			{
				public string CounterName { get; set; }
				public decimal Value { get; set; }
			}

			public Counters_Sum()
			{
				Map = applications => from application in applications
									  let performance = application.Performance
									  from counter in performance.Counters
									  select new
									  {
										  CounterName = counter.Name,
										  Value = counter.Value
									  };
				Reduce = results => from result in results
									group result by result.CounterName
										into g
										select new
										{
											CounterName = g.Key,
											Value = g.Sum(x => x.Value)
										};

			}
		}

		public class Counters_Avg : AbstractIndexCreationTask<Application, Counters_Avg.Result>
		{
			public class Result
			{
				public string CounterName { get; set; }
				public int Count { get; set; }
				public decimal AvgValue { get; set; }
			}

			public Counters_Avg()
			{
				Map = applications => from application in applications
									  let performance = application.Performance
									  from counter in performance.Counters
									  select new
									  {
										  CounterName = counter.Name,
										  AvgValue = counter.Value,
										  Count = 1

									  };
				Reduce = results => from result in results
									group result by result.CounterName
										into g
										let sum = g.Sum(x => x.Count)
										select new
										{
											CounterName = g.Key,
											AvgValue = g.Sum(x => x.AvgValue) / sum,
											Count = sum
										};

			}
		}

		private static void Main(string[] args)
		{
			using (var store = new DocumentStore { Url = "http://localhost:8080", DefaultDatabase = "Applications" }.Initialize())
			{
				store.DatabaseCommands.Admin.EnsureDatabaseExists("Applications");

				new ApplicationByAuthor().Execute(store);
				new ApplicationByName().Execute(store);
				new Counters_Sum().Execute(store);
				new Counters_Avg().Execute(store);

				for (int i = 0; i < 50000; i++)
				{
					using (var session = store.OpenSession())
					{
						for (int j = 0; j < 30; j++)
						{
							session.Store(new Application()
							{
								Name = "Application/" + (i * j) % 200,
								Author = "Author/" + (i * j) % 100,
								Performance = new Performance()
								{
									Counters = new List<Counter>()
									{
										new Counter()
										{
											DateTime = DateTime.Now,
											Name = "Counter/" + (i * j) % 1000,
											Value = (i*j) % 10000,
										},
										new Counter()
										{
											DateTime = DateTime.Now,
											Name = "Counter/" + (i * j) % 2000,
											Value = (i*j) % 100,
										},
										new Counter()
										{
											DateTime = DateTime.Now,
											Name = "Counter/" + (i * j) % 5000,
											Value = (i*j) % 500,
										}
									}
								}
							});
						}

						session.SaveChanges();
					}
				}
			}
		}
	}
}