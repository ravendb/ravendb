using System;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
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
    }
}
