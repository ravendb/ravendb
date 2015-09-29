using System;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Bundles.Versioning;
using Xunit;

namespace Raven.Tests.Bundles.CompressionAndEncryption
{
	public class Crud : CompressionAndEncryption
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
	}
}