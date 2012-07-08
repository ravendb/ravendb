using System;
using Raven.Bundles.Tests.Versioning;
using Xunit;

namespace Raven.Bundles.Tests.Encryption
{
	public class Crud : Encryption
	{
		// store & load
		// transactions
		// map reduce
		// simple indexes
		// queries

		[Fact]
		public void StoreAndLoad()
		{
			var company = new Company { Name = "Company Name" };
			using (var session = documentStore.OpenSession())
			{
				session.Store(company);
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				Assert.Equal(company.Name, session.Load<Company>(1).Name);
			}
		}
	}
}