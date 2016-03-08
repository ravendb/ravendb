using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem.Notifications;
using Raven.Json.Linq;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class QueryShouldNotAppendMetaData : RavenTestBase
    {
        [Fact]
        public void querying_and_saving_should_not_change_metadata()
        {
            using (var store = NewDocumentStore())
            {

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
                    session.SaveChanges();
                }

                var loadresult = store.DatabaseCommands.Get(new[] { "customers/1" }, new string[] { });
                var originalMetadata = (RavenJObject)loadresult.Results[0]["@metadata"];

                using (var session = store.OpenSession())
                {
                    var john = session.Query<Customer>().Customize(c => c.WaitForNonStaleResults()).FirstOrDefault();
                    Assert.NotNull(john);
                    john.Name = "John Doe";
                    session.SaveChanges();
                }
                loadresult = store.DatabaseCommands.Get(new[] { "customers/1" }, new string[] { });
                var newMetadata = (RavenJObject)loadresult.Results[0]["@metadata"];

                var expectedMetadataKeys = new[] { Constants.RavenClrType, Constants.RavenEntityName, Constants.LastModified, Constants.RavenLastModified, "@id", "@etag" };
                var metadataKeysThatShouldNotBePresent = newMetadata.Keys.Except(expectedMetadataKeys).ToList();

                Assert.False(metadataKeysThatShouldNotBePresent.Any(), "These metadata keys should not have been stored: " + string.Join(", ", metadataKeysThatShouldNotBePresent));

                // The metadata shouldn't have changed due to the second SaveChanges.
                foreach (var changingHeader in new[] { "@etag", Constants.RavenLastModified, Constants.LastModified })
                {
                    originalMetadata.Remove(changingHeader);
                    newMetadata.Remove(changingHeader);
                }

                Assert.Equal(originalMetadata, newMetadata);

            }
        }

        public class Customer
        {
            public string Name { get; set; }
            public string Address { get; set; }
        }
    }
}