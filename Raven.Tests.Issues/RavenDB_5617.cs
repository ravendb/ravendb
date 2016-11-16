using System;
using System.Linq;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_5617 : RavenTestBase
    {
        [Fact]
        public void CanAutomaticallyWaitForIndexes_ForSpecificIndex()
        {
            using (var store = NewDocumentStore())
            {
                var userByReverseName = new RavenDB_4903.UserByReverseName();
                userByReverseName.Execute(store);
                using (var s = store.OpenSession())
                {
                    s.Advanced.WaitForIndexesAfterSaveChanges(
                        timeout: TimeSpan.FromSeconds(30),
                        indexes: new[] { userByReverseName.IndexName },
                        throwOnTimeout: true);

                    s.Store(new RavenDB_4903.User { Name = "Oren" });

                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    Assert.NotEmpty(s.Query<RavenDB_4903.User, RavenDB_4903.UserByReverseName>().Where(x => x.Name == "nerO").ToList());
                }
            }
        }
    }
}
