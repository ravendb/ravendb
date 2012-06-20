using System;
using System.Linq;
using Raven.Imports.Newtonsoft.Json;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Client.Linq.Indexing;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class transform_results_loading_document_from_property_on_loaded_document : RavenTest
	{
		public abstract class Account
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public string Number { get; set; }
		}

		public class ClickTransaction
		{
			public string Id { get; set; }

			public string AccountId { get; set; }

			public int Quantity { get; set; }

			public string DesignName { get; set; }

			public string DesignId { get; set; }

			public string UserEmail { get; set; }

			public string ClickAllocationId { get; set; }

			public DateTime Date { get; set; }

			public string ComputerName { get; set; }

			public string Type { get; set; }
		}

		public class ClickTransactionDailyDebitReportResult
		{
			public string AccountDealerId { get; set; }
			public string AccountDealerName { get; set; }
			public string AccountDealerNumber { get; set; }
			public string AccountId { get; set; }
			public string AccountName { get; set; }
			public string AccountNumber { get; set; }
			public string AccountType { get; set; }
			public DateTime ClickTransactionDate { get; set; }
			public int ClickTransactionQuantity { get; set; }
			public string DesignName { get; set; }
			public string UserEmail { get; set; }
		}

		public class ClickTransactions_DailyDebitReport : AbstractIndexCreationTask<ClickTransaction, ClickTransactions_DailyDebitReport.Result>
		{
			public class Result
			{
				public string AccountId { get; set; }
				public DateTime Date { get; set; }
				public string DesignName { get; set; }
				public int Quantity { get; set; }
				public string Type { get; set; }
				public string UserEmail { get; set; }
			}

			public ClickTransactions_DailyDebitReport()
			{
				Map = docs =>
					from doc in docs
					where doc.Type == "Debit"
					select new
					{
						doc.AccountId,
						doc.Date,
						doc.DesignName,
						doc.Quantity,
						doc.UserEmail,
					};

				Reduce = results =>
					from result in results
					group result by new { result.AccountId, result.DesignName, result.UserEmail, Date = result.Date.ToString("yyyyMMdd") } into g
					select new
					{
						g.Key.AccountId,
						Date = g.Select(x => x.Date.Date).FirstOrDefault(),
						g.Key.DesignName,
						Quantity = g.Sum(x => x.Quantity),
						g.Key.UserEmail
					};

				TransformResults = (database, results) =>
					from result in results
					let account = database.Load<Account>(result.AccountId)
					let customer = account.IfEntityIs<Customer>("Accounts")
					let dealer = database.Load<Dealer>(customer.DealerId)
					select new
					{
						AccountId = account.Id,
						AccountName = account.Name,
						AccountNumber = account.Number,
						AccountDealerId = dealer != null ? dealer.Id : null,
						AccountDealerName = dealer != null ? dealer.Name : null,
						AccountDealerNumber = dealer != null ? dealer.Number : null,
						ClickTransactionQuantity = result.Quantity,
						ClickTransactionDate = result.Date,
						ClickTransactionType = result.Type,
						DesignName = result.DesignName,
						UserEmail = result.UserEmail,
						customer = customer
					};
			}
		}

		public class Customer : Account
		{
			public string DealerId { get; set; }
		}

		public class Dealer : Account
		{
		}

		protected override void ModifyStore(Raven.Client.Embedded.EmbeddableDocumentStore documentStore)
		{
			base.ModifyStore(documentStore);
			documentStore.Conventions = new DocumentConvention
			{
				CustomizeJsonSerializer = serializer => serializer.TypeNameHandling = TypeNameHandling.All,
				DefaultQueryingConsistency = ConsistencyOptions.QueryYourWrites,
				FindTypeTagName = type =>
				{
					if (typeof(Account).IsAssignableFrom(type))
						return "Accounts";

					return DocumentConvention.DefaultTypeTagName(type);
				},
			};
		}

		private void TestScenario(Raven.Client.Embedded.EmbeddableDocumentStore store)
		{
			using (var session = store.OpenSession())
			{
				session.Store(new Dealer { Id = "accounts/1", Name = "Dealer" });
				session.Store(new Customer { Id = "accounts/2", Name = "Customer", DealerId = "accounts/1" });
				session.Store(new ClickTransaction
				{
					AccountId = "accounts/2",
					Date = DateTime.Today,
					Quantity = 100,
					Type = "Debit",
					DesignName = "N009876",
					UserEmail = "user@customer.com"
				});
				session.SaveChanges();
			}

			WaitForIndexing(store);

			using (var session = store.OpenSession())
			{
				var result = session.Advanced.LuceneQuery<ClickTransactionDailyDebitReportResult, ClickTransactions_DailyDebitReport>()
					.WaitForNonStaleResultsAsOfNow()
					.ToArray()
					.FirstOrDefault();

				Assert.Equal("Dealer", result.AccountDealerName);
			}
		}

		[Fact]
		public void defining_TransformResults_manually_with_text()
		{
			using (var store = NewDocumentStore())
			{
				var indexTask = new ClickTransactions_DailyDebitReport() { Conventions = store.Conventions };
				var definition = indexTask.CreateIndexDefinition();
				definition.TransformResults = @"from result in results
let account = Database.Load(result.AccountId)
let dealer = Database.Load(account.DealerId)
select new
{
    AccountId = account.Id,
    AccountName = account.Name,
    AccountNumber = account.Number,
    AccountDealerId = dealer != null ? dealer.Id : null,
    AccountDealerName = dealer != null ? dealer.Name : null,
    AccountDealerNumber = dealer != null ? dealer.Number : null,
    ClickTransactionQuantity = result.Quantity,
    ClickTransactionDate = result.Date,
    ClickTransactionType = result.Type,
    DesignName = result.DesignName,
    UserEmail = result.UserEmail
}";
				store.DatabaseCommands.PutIndex(indexTask.IndexName, definition);

				TestScenario(store);
			}
		}

		[Fact]
		public void defining_TransformResults_using_features_of_AbstractIndexCreationTask()
		{
			using (var store = NewDocumentStore())
			{
				var clickTransactionsDailyDebitReport = new ClickTransactions_DailyDebitReport();
				clickTransactionsDailyDebitReport.Execute(store);

				TestScenario(store);
			}
		}
	}
}