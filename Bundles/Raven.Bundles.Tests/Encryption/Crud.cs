using System;
using System.IO;
using System.Text;
using System.Transactions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Bundles.Tests.Versioning;
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
			const string SecondCompany = "SecondCompany";
			const string ThirdCompany = "ThirdCompany";
			const string FourthCompany = "FourthCompany";

			using (var session = documentStore.OpenSession())
			using (var tx = new TransactionScope())
			{
				session.Store(new Company { Name = FirstCompany });
				session.Store(new Company { Name = SecondCompany });
				session.SaveChanges();
				tx.Complete();
			}

			using (var session = documentStore.OpenSession())
			using (new TransactionScope())
			{
				session.Store(new Company { Name = ThirdCompany });
				session.Store(new Company { Name = FourthCompany });
				session.SaveChanges();
				// this transaction is not committed
			}

			using (var session = documentStore.OpenSession())
			{
				Assert.Equal(FirstCompany, session.Load<Company>(1).Name);
				Assert.Equal(SecondCompany, session.Load<Company>(2).Name);
				Assert.Equal(null, session.Load<Company>(3));
				Assert.Equal(null, session.Load<Company>(4));
			}

			AssertPlainTextIsNotSavedInDatabase(FirstCompany, SecondCompany, ThirdCompany, FourthCompany);
		}
	}
}