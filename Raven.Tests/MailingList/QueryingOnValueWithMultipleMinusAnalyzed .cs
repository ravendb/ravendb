using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Database.Indexing;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class QueryingOnValueWithMultipleMinusAnalyzed : RavenTest
	{
		[Fact]
		public void CanPerformQueryWithDashesInTerm()
		{
			using (var store = NewDocumentStore())
			{
				var indexDefinition = new IndexDefinitionBuilder<Product>()
				{
					Map = products => from product in products
										select new
										{
											Query = new object[]
											{
												product.ItemNumber,
												product.ItemDescription,

											},
											product.ProductId

										},
					Indexes =
					{
						{x => x.Query, FieldIndexing.Analyzed}
					},
					Analyzers =
					{
						{x => x.Query, typeof (LowerCaseWhitespaceAnalyzer).AssemblyQualifiedName}
					}

				}.ToIndexDefinition(store.Conventions);

				store.DatabaseCommands.PutIndex("someIndex", indexDefinition);


				var prodOne = new Product
				{
					ProductId = "one",
					ItemNumber = "Q9HT180-Z-Q",
					ItemDescription = "PILLOW PROTECTOR QUEEN"
				};
				var prodTwo = new Product
				{
					ProductId = "two",
					ItemNumber = "Q9HT180-Z-U",
					ItemDescription = "PILLOW PROTECTOR STANDARD"
				};
				var prodThree = new Product
				{
					ProductId = "three",
					ItemNumber = "Q9HT180-Z-K",
					ItemDescription = "PILLOW PROTECTOR KING"
				};

				using (var session = store.OpenSession())
				{
					session.Store(prodOne);
					session.Store(prodTwo);
					session.Store(prodThree);
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var prods = session.Advanced.LuceneQuery<Product>("someIndex")
						.WaitForNonStaleResults()
						.WhereStartsWith(x => x.Query, "Q9HT180-Z-K")
						.ToList();

					Assert.Equal(1, prods.Count);
				}
			}
		}

		public class Product
		{
			public string ProductId { get; set; }

			public string Query { get; set; }

			public string ItemNumber { get; set; }

			public string ItemDescription { get; set; }

		}
	}

}