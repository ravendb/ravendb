using System.IO;
using System.Linq;
using FastTests;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8773 : RavenTestBase
    {
        public RavenDB_8773(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanDealWithDocumentsAndAttachmentsHavingHashCharacter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "#" }, "#");

                    session.SaveChanges();

                    var user = session.Load<User>("#");

                    Assert.NotNull(user);

                    var users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Name == "#").ToList();

                    Assert.Equal("#", users[0].Id);
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Attachments.Store("#", "#", new MemoryStream(new byte[] { 1 }));

                    session.SaveChanges();

                    using (var attachmentResult = session.Advanced.Attachments.Get("#", "#"))
                    {
                        Assert.Equal(1, attachmentResult.Details.Size);
                    }
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Attachments.Delete("#", "#");;

                    session.SaveChanges();

                    using (var attachment = session.Advanced.Attachments.Get("#", "#"))
                    {
                        Assert.Null(attachment);
                    }

                    session.Delete("#");

                    Assert.Null(session.Load<User>("#"));
                }
            }
        }
    }
}
