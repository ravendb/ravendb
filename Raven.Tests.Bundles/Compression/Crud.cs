using System;
using Raven.Abstractions.Data;
using Raven.Database.Config;
using Raven.Json.Linq;
using Raven.Tests.Bundles.Versioning;
using Xunit;

namespace Raven.Tests.Bundles.Compression
{
	public class Crud : Compression
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
				Assert.Equal(CompanyName, session.Load<Company>(1).Name);
			}

			AssertPlainTextIsNotSavedInDatabase_ExceptIndexes(CompanyName);
		}
	}
}