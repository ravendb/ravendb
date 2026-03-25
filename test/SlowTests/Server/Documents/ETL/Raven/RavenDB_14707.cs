using System;
using System.Threading.Tasks;
using Xunit;
using Raven.Tests.Core.Utils.Entities;
using FastTests;
using Tests.Infrastructure;

namespace SlowTests.Server.Documents.ETL.Raven
{
    public class RavenDB_14707 : RavenTestBase
    {
        public RavenDB_14707(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Should_delete_existing_document_when_filtered_by_script(Options options)
        {
            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore())
            {
                Etl.AddEtl(src, dest, "Users", script: @"if (this.Name == 'Joe Doe') loadToUsers(this);");

                var etlDone = Etl.WaitForEtlToComplete(src);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe Doe"
                    }, "users/1");

                    session.SaveChanges();
                }

                await etlDone.WaitAsync(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    Assert.NotNull(user);
                }

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    user.Name = "John Doe";
                    session.SaveChanges();
                }

                await etlDone.WaitAsync(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    Assert.Null(user);
                }
            }
        }
    }
}
