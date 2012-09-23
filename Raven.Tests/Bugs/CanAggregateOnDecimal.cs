using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Xunit;
using Raven.Client.Linq;

namespace Raven.Tests.Bugs
{
	public class CanAggregateOnDecimal : RavenTest
	{
		public class DecimalAggregation_Map : AbstractIndexCreationTask<Bank, BankTotal>
		{
			public DecimalAggregation_Map()
			{
				Map = banks => from bank in banks
							   select new { Total = bank.Accounts.Sum(x => x.Amount) };
				Store(x => x.Total, FieldStorage.Yes);
			}
		}

		public class DecimalAggregation_Reduce : AbstractIndexCreationTask<Bank, BankTotal>
		{
			public DecimalAggregation_Reduce()
			{
				Map = banks => from bank in banks
							   select new { Total = bank.Accounts.Sum(x => x.Amount) };
				Reduce = results => from bankTotal in results
				                    group bankTotal by 1
				                    into g
				                    select new {Total = g.Sum(x => x.Total)};
			}
		}

		public class BankTotal
		{
			public decimal Total { get; set; }
		}

		public class Bank
		{
			public Account[] Accounts { get; set; }
		}
		public class Account
		{
			public decimal Amount { get; set; }
		}

		[Fact]
		public void MapOnly()
		{
			using(var store = NewDocumentStore())
			{
				new DecimalAggregation_Map().Execute(store);
				using(var session = store.OpenSession())
				{
					session.Store(new Bank
					{
						Accounts = new[]
						{
							new Account {Amount = 0.1m},
							new Account {Amount = 321.312m},
						}
					});
					session.SaveChanges();
				}
				WaitForIndexing(store);
				Assert.Empty(store.DocumentDatabase.Statistics.Errors);
				using (var session = store.OpenSession())
				{
					var bankTotal = session.Query<BankTotal, DecimalAggregation_Map>()
						.Customize(x=>x.WaitForNonStaleResults())
						.AsProjection<BankTotal>()
						.Single();

					Assert.Equal(321.412m, bankTotal.Total);
				}
			}
		}

		[Fact]
		public void Reduce()
		{
			using (var store = NewDocumentStore())
			{
				new DecimalAggregation_Reduce().Execute(store);
				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 5; i++)
					{
						session.Store(new Bank
						{
							Accounts = new[]
						{
							new Account {Amount = 0.1m},
							new Account {Amount = 321.312m},
						}
						});
					}
					session.SaveChanges();
				}
				WaitForIndexing(store);
				Assert.Empty(store.DocumentDatabase.Statistics.Errors);
				using (var session = store.OpenSession())
				{
					var bankTotal = session.Query<BankTotal, DecimalAggregation_Reduce>()
						.Customize(x => x.WaitForNonStaleResults())
						.Single();

					Assert.Equal(1607.060m, bankTotal.Total);
				}
			}
		}
	}
}