using System.Linq;
using FastTests;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Queries
{
    public class QueryingOnMetadata : RavenTestBase
    {
        public QueryingOnMetadata(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanQueryOnNullableProperty()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var u1 = new User();
                    session.Store(u1);
                    var metadata = session.Advanced.GetMetadataFor(u1);
                    metadata["Errored"] = true;
                    metadata["JobId"] = "12cd80f2-34b0-4dd9-8464-d1cefad07256";

                    var u2 = new User();
                    session.Store(u2);
                    var metadata2 = session.Advanced.GetMetadataFor(u2);
                    // doesn't have metadata property
                    //metadata2["Errored"] = true;
                    metadata2["JobId"] = "12cd80f2-34b0-4dd9-8464-d1cefad07256";

                    var u3 = new User();
                    session.Store(u3);
                    var metadata3 = session.Advanced.GetMetadataFor(u3);
                    metadata2["Errored"] = false;
                    metadata3["JobId"] = "12cd80f2-34b0-4dd9-8464-d1cefad07256";
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Advanced.DocumentQuery<User>()
                        .WaitForNonStaleResults()
                        .WhereEquals("@metadata.JobId", "12cd80f2-34b0-4dd9-8464-d1cefad07256")
                        .AndAlso()
                        .WhereEquals("@metadata.Errored", false)
                        .ToArray();

                    Assert.Equal(1, users.Length);
                }
            }
        }
    }
}
