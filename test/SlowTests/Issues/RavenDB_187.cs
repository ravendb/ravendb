using FastTests;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using SlowTests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_187 : RavenTestBase
    {
        [Fact(Skip = "Delete marker is no longer supported")]
        public void ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                store.DatabaseCommands.Put("users/1", null, new RavenJObject(), new RavenJObject
                {
                    {Constants.Headers.RavenDeleteMarker, "true"}
                });

                using (var s = store.OpenSession())
                {
                    s.Advanced.UseOptimisticConcurrency = true;
                    s.Store(new User());
                    s.SaveChanges();
                }
            }
        }
    }
}
