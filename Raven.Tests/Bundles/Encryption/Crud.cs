using System;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Bundles.Versioning;
using Xunit;

namespace Raven.Tests.Bundles.Encryption
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