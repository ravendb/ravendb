using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.Issues
{
    public class CustomerMetadataAfterSaveChanges : RavenTestBase
    {
        [Fact]
        public async Task ShouldBeAvailable()
        {
            using (var store = GetDocumentStore())
            {
                string name = null;
                store.OnAfterSaveChanges += (object sender, AfterSaveChangesEventArgs e) => {
                    name = (string)e.DocumentMetadata["Name"];
                };
                store.Initialize();

                using (var session = store.OpenAsyncSession())
                {
                    var user = new { Name = "Oren" };
                    await session.StoreAsync(user, "users/oren");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    metadata.Add("Name", "FooBar");

                    await session.SaveChangesAsync();
                }

                Assert.Equal("FooBar", name);
            }
        }
    }
}
