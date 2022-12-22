using System;
using SlowTests.Core.Utils.Entities;
using SlowTests.Server.Documents.ETL;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19466:EtlTestBase 
{
    public RavenDB_19466(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void CanModifyDocumentMetadataUsingEtlTransformScriptWithDeleteDocumentsBehaviorFunction()
    {
        using (var src = GetDocumentStore())
        using (var dest = GetDocumentStore())
        {
            AddEtl(src, dest, "Users", script: @"function deleteDocumentsBehavior(docId, collection, deleted) {
                                                                return false; // don't send any deletes to the other side
                                                            }
                                                            var metadata = getMetadata(this);
                                                            metadata[""TestETLMetadataReference""] = ""HelloFromTransformScript"";
                                                            loadToUsers(this);");

            var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

            using (var session = src.OpenSession())
            {
                session.Store(new User {Name = "Gracjan"});

                session.SaveChanges();
            }
            etlDone.Wait(TimeSpan.FromMinutes(1));

            using (var session = dest.OpenSession())
            {
                var user = session.Load<User>("users/1-A");
                Assert.NotNull(user);
                Assert.Equal("HelloFromTransformScript", session.Advanced.GetMetadataFor(user)["TestETLMetadataReference"]);
            }
            
            using (var session = src.OpenSession())
            {
                session.Delete("users/1-A");
                session.SaveChanges();
            }
            
            etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);
            etlDone.Wait(TimeSpan.FromMinutes(1));
            
            using (var session = dest.OpenSession())
            {
                var user = session.Load<User>("users/1-A");
                Assert.NotNull(user);
                Assert.Equal("HelloFromTransformScript", session.Advanced.GetMetadataFor(user)["TestETLMetadataReference"]);
            }
        }
    }

}
