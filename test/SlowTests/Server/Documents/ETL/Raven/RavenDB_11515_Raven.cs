using System;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Server.Documents.ETL.Raven
{
    public class RavenDB_11515_Raven : EtlTestBase
    {
        [Fact]
        public void Can_filter_out_deletions_of_documents()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, "Users", script:
@"
    loadToUsers(this);

    function deleteDocumentsOfUsersBehavior(docId) {
        return false;
    }
");

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe"
                    }, "users/1");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                }

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                }
            }
        }
    }
}
