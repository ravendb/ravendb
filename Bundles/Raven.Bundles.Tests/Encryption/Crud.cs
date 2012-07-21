using System;
using System.IO;
using System.Text;
using System.Transactions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Bundles.Tests.Versioning;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Bundles.Tests.Encryption
{
	public class Crud : Encryption
	{
		[Fact]
		public void StoreAndLoad()
		{
			const string CompanyName = "Company Name";
			var company = new Company { Name = CompanyName };
			using (var session = documentStore.OpenSession())
			{
				session.Store(company);
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				Assert.Equal(company.Name, session.Load<Company>(1).Name);
			}

			AssertPlainTextIsNotSavedInDatabase(CompanyName);
		}

		[Fact]
		public void Transactional()
		{
			const string FirstCompany = "FirstCompany";

			// write in transaction
			documentStore.DatabaseCommands.Put("docs/1", null,
											   new RavenJObject
			                                   	{
			                                   		{"Name", FirstCompany}
			                                   	},
											   new RavenJObject
			                                   	{
			                                   		{
			                                   			"Raven-Transaction-Information", Guid.NewGuid() + ", " + TimeSpan.FromMinutes(1)
			                                   		}
			                                   	});

			var jsonDocument = documentStore.DatabaseCommands.Get("docs/1");
			Assert.True(jsonDocument.Metadata.Value<bool>(Constants.RavenDocumentDoesNotExists));


			AssertPlainTextIsNotSavedInDatabase(FirstCompany);
		}
	}
}