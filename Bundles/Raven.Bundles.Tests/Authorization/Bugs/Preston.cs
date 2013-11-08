extern alias client;
using System.Linq;
using client::Raven.Client.Authorization;
using client::Raven.Bundles.Authorization.Model;
using Raven.Client.Exceptions;
using Raven.Client;
using Raven.Client.Linq;
using Raven.Client.Indexes;
using Xunit;
using Raven.Abstractions.Data;
using System.Collections.Generic;
using Raven.Abstractions.Data;

namespace Raven.Bundles.Tests.Authorization.Bugs
{
	public class Preston : AuthorizationTest
	{
        [Fact]
        public void CannotReadDocumentWhenTransformIsAppliedWithoutPermissionToIt()
        {
            new CompanyTransformer().Execute(store);
            var company = new Company
            {
                Name = "Hibernating Rhinos"
            };
            using (var s = store.OpenSession())
            {

                s.Store(new AuthorizationUser
                {
                    Id = UserId,
                    Name = "Ayende Rahien",
                });

                s.Store(company);

                s.SetAuthorizationFor(company, new DocumentAuthorization());// deny everyone

                s.SaveChanges();
            }

            using (var s = store.OpenSession())
            {
                s.SecureFor(UserId, "Company/Bid");

                var companyListTransform = s.Query<Company>().Where(c => c.Name.StartsWith("Hibernating")).TransformWith<CompanyTransformer, CompanyTransformer.TransformedCompany>().ToList();
                var companyListNoTransform = s.Query<Company>().Where(c => c.Name.StartsWith("Hibernating")).ToList();

                Assert.Equal(companyListNoTransform.Count, companyListTransform.Count);

                var readVetoException = Assert.Throws<ReadVetoException>(() => s.Load<CompanyTransformer, CompanyTransformer.TransformedCompany>(company.Id));

                Assert.Equal(@"Document could not be read because of a read veto.
The read was vetoed by: Raven.Bundles.Authorization.Triggers.AuthorizationReadTrigger
Veto reason: Could not find any permissions for operation: Company/Bid on companies/1 for user Authorization/Users/Ayende.
No one may perform operation Company/Bid on companies/1
", readVetoException.Message);
            }
        }

    }
    public class CompanyTransformer : AbstractTransformerCreationTask<Company>
    {
        public class TransformedCompany
        {
            public string CompanyId { get; set; }
        }
        public CompanyTransformer()
        {
            TransformResults = companies =>
                from company in companies
                select new TransformedCompany
                {
                    CompanyId = company.Id
                };
        }
    }

}