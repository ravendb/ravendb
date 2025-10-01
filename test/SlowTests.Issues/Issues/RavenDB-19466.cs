using System;
using FastTests;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19466 : RavenTestBase 
{
    public RavenDB_19466(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Etl)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public void CanModifyDocumentMetadataUsingEtlTransformScriptWithDeleteDocumentsBehaviorFunction(Options options)
    {
        using (var src = GetDocumentStore(options))
        using (var dest = GetDocumentStore())
        {
            Etl.AddEtl(src, dest, "Users", script: @"function deleteDocumentsBehavior(docId, collection, deleted) {
                                                                return false; // don't send any deletes to the other side
                                                            }
                                                            var metadata = getMetadata(this);
                                                            metadata[""TestETLMetadataReference""] = ""HelloFromTransformScript"";
                                                            loadToUsers(this);");

            var etlDone = Etl.WaitForEtlToComplete(src);

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

            etlDone = Etl.WaitForEtlToComplete(src);
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
