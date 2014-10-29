using System.Linq;

using Raven.Client.Exceptions;
using Raven.Client.Linq;
using Raven.Client.Indexes;

using Xunit;

namespace Raven.Tests.Bundles.Authorization.Bugs
{
    extern alias client;

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
            using (var s = store.OpenSession(DatabaseName))
            {

                s.Store(new client::Raven.Bundles.Authorization.Model.AuthorizationUser
                {
                    Id = UserId,
                    Name = "Ayende Rahien",
                });

                s.Store(company);

                client::Raven.Client.Authorization.AuthorizationClientExtensions.SetAuthorizationFor(s, company, new client::Raven.Bundles.Authorization.Model.DocumentAuthorization());// deny everyone

                s.SaveChanges();
            }

            using (var s = store.OpenSession(DatabaseName))
            {
                client::Raven.Client.Authorization.AuthorizationClientExtensions.SecureFor(s, UserId, "Company/Bid");

                var companyListTransform = s.Query<Company>()
                    .Where(c => c.Name.StartsWith("Hibernating"))
                    .TransformWith<CompanyTransformer, CompanyTransformer.TransformedCompany>()
                    .Customize(c => c.WaitForNonStaleResults())
                    .ToList();

                var companyListNoTransform = s.Query<Company>()
                    .Where(c => c.Name.StartsWith("Hibernating"))
                    .Customize(c => c.WaitForNonStaleResults())
                    .ToList();

                Assert.Equal(companyListNoTransform.Count, companyListTransform.Count);

                var readVetoException = Assert.Throws<ReadVetoException>(
                    () =>
                    {
                        s.Load<Company>(company.Id);
                    });

                Assert.Equal(@"Document could not be read because of a read veto.
The read was vetoed by: Raven.Bundles.Authorization.Triggers.AuthorizationReadTrigger
Veto reason: Could not find any permissions for operation: Company/Bid on companies/1 for user Authorization/Users/Ayende.
No one may perform operation Company/Bid on companies/1
", readVetoException.Message);

                readVetoException = Assert.Throws<ReadVetoException>(
                    () => {
                              s.Load<CompanyTransformer, CompanyTransformer.TransformedCompany>(company.Id);
                    });

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