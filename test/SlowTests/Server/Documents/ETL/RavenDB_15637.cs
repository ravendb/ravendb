using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.ETL
{
    public class RavenDB_15637 : RavenTestBase
    {
        public RavenDB_15637(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [InlineData(@"if(this.Age % 2 === 0)
    return;
if(this.Name == 'Sus')
    return;
loadToUsers(this);

function deleteDocumentsBehavior(docId, collection, deleted){
return deleted;
}")]
        [InlineData(@"if(this.Age % 2 === 0)
    return;
if(this.Name == 'Sus')
    return;
loadToUsers(this);

function deleteDocumentsOfUsersBehavior(docId, deleted){
return deleted;
}")]
        public async Task ShouldNotDeleteDestinationDocumentWhenFilteredOutOfLoad(string script)
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                using (var session = dest.OpenSession())
                {
                    session.Store(new User() {Name = "Crew Mate", Age = 32});
                    session.SaveChanges();
                }

                using (var session = src.OpenSession())
                {
                    session.Store(new User() {Name = "Crew Mate", Age = 32});
                    session.Store(new User() {Name = "Sus", Age = 31});
                    session.SaveChanges();

                    Etl.AddEtl(src, dest, "Users", script);
                    var etlDone = Etl.WaitForEtlToComplete(src);
                    await etlDone.WaitAsync(timeout:TimeSpan.FromSeconds(10));
                }

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1-A"));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [InlineData(@"if(this.Age % 2 === 0)
    return;
if(this.Name == 'Sus')
    return;
loadToUsers(this);

function deleteDocumentsBehavior(docId, collection, deleted) {
return !deleted;
}")]
        [InlineData(@"if(this.Age % 2 === 0)
    return;
if(this.Name == 'Sus')
    return;
loadToUsers(this);

function deleteDocumentsOfUsersBehavior(docId, deleted) {
return !deleted;
}")]
        public async Task ShouldDeleteDestinationDocumentWhenFilteredOutOfLoad(string script)
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                using (var session = dest.OpenSession())
                {
                    session.Store(new User() {Name = "Crew Mate", Age = 32});
                    session.SaveChanges();

                }

                using (var session = src.OpenSession())
                {
                    session.Store(new User() {Name = "Crew Mate", Age = 32});
                    session.Store(new User() {Name = "Sus", Age = 31});
                    session.SaveChanges();
                }

                Etl.AddEtl(src, dest, "Users", script);
                var etlDone = Etl.WaitForEtlToComplete(src);
                await etlDone.WaitAsync(timeout:TimeSpan.FromSeconds(30));

                using (var session = dest.OpenSession())
                {
                    Assert.Null(session.Load<User>("users/1-A"));
                }
            }
        }
    }
}
