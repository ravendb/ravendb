using FastTests;
using Raven.Client.Documents.Commands;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8267 : RavenTestBase
    {
        public RavenDB_8267(ITestOutputHelper output) : base(output)
        {
        }

        public class MyEntity
        {
            public string Username { get; set; }
            public string Password { get; set; }
            public string OrganizationId { get; set; }
            public string AgentDutyCode { get; set; }
        }
        [Fact]
        public unsafe void PropertySortOrderSustained()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new MyEntity
                    {
                        Username = "username",
                        Password = "password",
                        AgentDutyCode = "code",
                        OrganizationId = "rr"

                    }, "things/1");
                    session.SaveChanges();
                }

                GetDocumentsCommand getDocsCommand = new GetDocumentsCommand("things/1", null, false);
                store.Commands().Execute(getDocsCommand);
                var res = getDocsCommand.Result.Results[0] as BlittableJsonReaderObject;
                
                var propertiesByInsertionOrder = res.GetPropertiesByInsertionOrder();

                var expectedOrder = new[] { "Username", "Password", "OrganizationId", "AgentDutyCode" };

                for (var i = 0; i < expectedOrder.Length; i++)
                {
                    BlittableJsonReaderObject.PropertyDetails propDetails = new BlittableJsonReaderObject.PropertyDetails();

                    res.GetPropertyByIndex(propertiesByInsertionOrder.Properties[i], ref propDetails);

                    Assert.Equal(expectedOrder[i], propDetails.Name.ToString());
                }

            }
        }
    }
}
