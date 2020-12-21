using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16037 : RavenTestBase
    {
        public RavenDB_16037(ITestOutputHelper output) : base(output)
        {
        }


        [Fact]
        public void ShouldReturnLastModifiedInUtc()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Grisha"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                        select new
                        {
                            Name = u.Name,
                            Metadata = RavenQuery.Metadata(u),
                        };

                    var list = query.ToList();

                    Assert.Equal(1, list.Count);

                    var lastModifiedFromProjection = list[0].Metadata.GetString(Constants.Documents.Metadata.LastModified);
                    Assert.EndsWith("Z", lastModifiedFromProjection);
                }
            }
        }
    }
}
