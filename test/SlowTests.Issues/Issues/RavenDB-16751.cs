using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_16751 : RavenTestBase
{
    public RavenDB_16751(ITestOutputHelper output) : base(output)
    {
        
    }

    [RavenFact(RavenTestCategory.Etl)]
    public async Task CanGetRevisionsCountInEtlScript()
    {
        // Arrange
        using (var src = GetDocumentStore())
        using (var dest = GetDocumentStore())
        {
            await RevisionsHelper.SetupRevisionsAsync(Server.ServerStore, src.Database);
            
            // Act
            using (var session = src.OpenSession())
            {
                session.Store(new User {Name = "Gracjan"});
                session.SaveChanges();
            }
            using (var session = src.OpenSession())
            {
                var user = session.Load<User>("users/1-A");
                user.LastName = "Sadowicz";
                session.SaveChanges();
            }
            using (var session = src.OpenSession())
            {
                var user = session.Load<User>("users/1-A");
                user.Name = "Graziano";
                session.SaveChanges();
            }
            
            Etl.AddEtl(src, dest, "Users", script: @"var metadata = getMetadata(this);
                                                            metadata[""RevisionsCountFromEtl""] = getRevisionsCount();
                                                            loadToUsers(this);");

            var etlDone = Etl.WaitForEtlToComplete(src);
            etlDone.Wait(TimeSpan.FromMinutes(1));
            
            // Assert
            List<User> revisions;
            
            using (var session = src.OpenSession())
            {
                var user = session.Load<User>("users/1-A");
                revisions = session.Advanced.Revisions.GetFor<User>(user.Id);
            }

            using (var session = dest.OpenSession())
            {
                var user = session.Load<User>("users/1-A");
                Assert.NotNull(user);
                var metadata = session.Advanced.GetMetadataFor(user);
                Assert.Equal((long)revisions.Count, metadata["RevisionsCountFromEtl"]);
            }
        }
    }
}
