using System.Linq;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class CustomAnalyzerStartsWithFailure : RavenTest
	{
		EmbeddableDocumentStore documentStore;

		public CustomAnalyzerStartsWithFailure()
		{
			documentStore = NewDocumentStore();

			documentStore.ExecuteIndex(new CustomerByName());

			using (IDocumentSession session = documentStore.OpenSession())
			{
				session.Store(new Customer() { Name = "Rogério" });
				session.Store(new Customer() { Name = "Rogerio" });
				session.Store(new Customer() { Name = "Paulo Rogerio" });
				session.Store(new Customer() { Name = "Paulo Rogério" });
				session.SaveChanges();
			}
		}

		[Fact]
		public void query_customanalyzer_with_equals()
		{
			using (IDocumentSession session = documentStore.OpenSession())
			{
				// Test 1
				// Using "== Rogério" works fine
				var results1 = session.Query<Customer, CustomerByName>()
					.Customize(x => x.WaitForNonStaleResults())
					.Where(x => x.Name == "Rogério");

				Assert.Equal(results1.Count<Customer>(), 4);
			}
		}

		[Fact]
		public void query_customanalyzer_with_starswith()
		{
			using (IDocumentSession session = documentStore.OpenSession())
			{

				WaitForUserToContinueTheTest(documentStore);
				// Test 2
				// Using ".StartsWith("Rogério")" is expected to bring same result from test1, but fails
				var results2 = session.Query<Customer, CustomerByName>()
					.Customize(x => x.WaitForNonStaleResults())
					.Where(x => x.Name.StartsWith("Rogério"));

				Assert.Equal(results2.Count<Customer>(), 4);
			}
		}

		public override void Dispose()
		{
			documentStore.Dispose();
			base.Dispose();
		}

		public class Customer
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		public class CustomerByName : AbstractIndexCreationTask<Customer>
		{
			public CustomerByName()
			{
				Map = customers => from customer in customers select new { customer.Name };
				Indexes.Add(x => x.Name, FieldIndexing.Analyzed);
				Analyzers.Add(x => x.Name, typeof(CustomAnalyzer).AssemblyQualifiedName);
			}
		}

		public class CustomAnalyzer : StandardAnalyzer
		{
			public CustomAnalyzer()
				: base(Lucene.Net.Util.Version.LUCENE_30)
			{
			}

			public override TokenStream TokenStream(string fieldName, System.IO.TextReader reader)
			{
				return new ASCIIFoldingFilter(base.TokenStream(fieldName, reader));
			}
		}

	}
}