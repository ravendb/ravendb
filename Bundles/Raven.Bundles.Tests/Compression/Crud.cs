using System;
using System.IO;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Bundles.Tests.Versioning;
using Xunit;

namespace Raven.Bundles.Tests.Compression
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
				Assert.Equal(company.Name, session.Load<Company>(1).Name);
			}

			AssertPlainTextIsNotSavedInDatabase(CompanyName);
		}

		[Fact(Skip = "TODO")]
		public void Transactional()
		{
			// TODO
		}
	}
}