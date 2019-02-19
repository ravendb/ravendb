using FastTests;
using Raven.Client.Documents.Commands;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8267 : RavenTestBase
    {
        public class MyEntity
        {
            public string Username { get; set; }
            public string Password { get; set; }
            public string OrganizationId { get; set; }
            public string AgentDutyCode { get; set; }
        }
        [Fact]
        public void PropertySortOrderSustained()
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

                var propertyNames = res.GetPropertyNames();
                WaitForUserToContinueTheTest(store);
                for (var i = 0; i < expectedOrder.Length; i++)
                {
                    var matchingPropertyId = propertiesByInsertionOrder.Properties.Array[i + propertiesByInsertionOrder.Properties.Offset];
                    BlittableJsonReaderObject.PropertyDetails propDetails = new BlittableJsonReaderObject.PropertyDetails();

                    res.GetPropertyByIndex(matchingPropertyId, ref propDetails);

                    Assert.Equal(expectedOrder[i], propDetails.Name.ToString());
                }

            }
        }
    }
}
