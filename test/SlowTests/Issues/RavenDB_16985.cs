using System.IO;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using FastTests.Graph;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16985 : RavenTestBase
    {
        public RavenDB_16985(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CheckIfHasChangesIsTrueAfterAddingAttachment()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    var user = new User();
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();

                    using var stream = new MemoryStream(Encoding.UTF8.GetBytes("my test text"));
                    session.Advanced.Attachments.Store(user, "my-test.txt", stream);

                    var hasChanges = session.Advanced.HasChanges;
                    Assert.True(hasChanges);
                }
            }
        }
    }
}
