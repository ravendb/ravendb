//-----------------------------------------------------------------------
// <copyright file="QueryingFromIndex.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Document;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class QueryingFromIndex : RavenTest
	{
		[Fact]
		public void LuceneQueryWithIndexIsCaseInsensitive()
		{
			using (var store = this.NewDocumentStore())
			{
				var definition = new IndexDefinitionBuilder<Company>
				{
					Map = docs => from doc in docs
								  select new
								  {
									  doc.Name
								  }
				}.ToIndexDefinition(store.Conventions);
				store.DatabaseCommands.PutIndex("CompanyByName",
												definition);

				using (var session = store.OpenSession())
				{
					session.Store(new Company { Name = "Google" });
					session.Store(new Company
					{
						Name =
							"HibernatingRhinos"
					});
					session.SaveChanges();

					var company =
						session.Advanced.LuceneQuery<Company>("CompanyByName")
							.Where("Name:Google")
							.WaitForNonStaleResults()
							.FirstOrDefault();

					Assert.NotNull(company);
				}
			}
		}

		[Fact]
		public void LinqQueryWithIndexIsCaseInsensitive()
		{
			using (var store = this.NewDocumentStore())
			{
				var definition = new IndexDefinitionBuilder<Company>
				{
					Map = docs => from doc in docs
								  select new
								  {
									  doc.Name
								  }
				}.ToIndexDefinition(store.Conventions);
				store.DatabaseCommands.PutIndex("CompanyByName",
												definition);

				using (var session = store.OpenSession())
				{
					session.Store(new Company { Name = "Google" });
					session.Store(new Company
					{
						Name =
							"HibernatingRhinos"
					});
					session.SaveChanges();

					var company =
						session.Query<Company>("CompanyByName")
							.Customize(x=>x.WaitForNonStaleResults())
							.Where(x=>x.Name == "Google")
							.FirstOrDefault();

					Assert.NotNull(company);
				}
			}
		}
	}
}
