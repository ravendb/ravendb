using Tests.Infrastructure;
using System;
using Xunit;
using Xunit.Abstractions;
using Raven.Tests.Core.Utils.Entities;

namespace SlowTests.Server.Documents.ETL
{
    public class RavenDB_15637 : EtlTestBase
    {
        public RavenDB_15637(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
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
        public void ShouldNotDeleteDestinationDocumentWhenFilteredOutOfLoad(string script)
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

                    AddEtl(src, dest, "Users", script);
                    var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);
                    etlDone.Wait(timeout:TimeSpan.FromSeconds(10));
                }

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1-A"));
                }
            }
        }
        
        const string _scriptShouldDelete1 = @"
if(this.Age % 2 === 0)
    return;
if(this.Name == 'Sus')
    return;
loadToUsers(this);

function deleteDocumentsBehavior(docId, collection, deleted) {
return !deleted;
}";

        const string _scriptShouldDelete2 = @"
if(this.Age % 2 === 0)
    return;
if(this.Name == 'Sus')
    return;
loadToUsers(this);

function deleteDocumentsOfUsersBehavior(docId, deleted) {
return !deleted;
}";

        
        [Theory]
        [RavenData(_scriptShouldDelete1, JavascriptEngineMode = RavenJavascriptEngineMode.All)]
        [RavenData(_scriptShouldDelete2, JavascriptEngineMode = RavenJavascriptEngineMode.All)]
        public void ShouldDeleteDestinationDocumentWhenFilteredOutOfLoad(Options options, string script)
        {

            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore(options))
            {
                using (var session = dest.OpenSession())
                {
                    session.Store(new User() { Name = "Crew Mate", Age = 32 });
                    session.SaveChanges();
                }

                using (var session = src.OpenSession())
                {
                    session.Store(new User() { Name = "Crew Mate", Age = 32 });
                    session.Store(new User() { Name = "Sus", Age = 31 });
                    session.SaveChanges();
                }

                AddEtl(src, dest, "Users", script);
                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);
                etlDone.Wait(timeout: TimeSpan.FromSeconds(30));

                using (var session = dest.OpenSession())
                {
                    var res = session.Load<User>("users/1-A");
                    Assert.Null(res);
                }
            }
        }
    }
}
