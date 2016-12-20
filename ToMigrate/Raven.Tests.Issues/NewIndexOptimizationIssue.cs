using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Database.Config;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class NewIndexOptimizationIssue : RavenTestBase
    {
        protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
        {
            //limit precompute batch total size, so the operation fails in the test
            configuration.MaxPrecomputedBatchTotalDocumentSizeInBytes = 5;
        }

        [Fact]
        public void AllDocumentsShouldBeIndexedEvenIfPrecomputedBatchFails()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Post
                    {
                        Title = "Querying document database"
                    });

                    session.Store(new Post
                    {
                        Title = "Introduction to RavenDB"
                    });

                    session.Store(new Post
                    {
                        Title = "NOSQL databases"
                    });

                    session.Store(new Post
                    {
                        Title = "MSSQL 2012"
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                store.DatabaseCommands.PutIndex("Posts/ByTitle", new IndexDefinition
                {
                    Map = "from post in docs.Posts select new { post.Title }",
                    Indexes = { { "Title", FieldIndexing.Analyzed } }
                });

                //should fail because the threshold is set to 100 bytes, and 
                //total size of existing documents is more than that
                WaitForIndexing(store);

                List<Post> fetchedPosts;
                using (var session = store.OpenSession())
                    fetchedPosts = session.Query<Post>("Posts/ByTitle").ToList();

                var expectedTitles = new[] { "Querying document database", "Introduction to RavenDB", "NOSQL databases", "MSSQL 2012" };
                var fetchedTitles = fetchedPosts.Select(x => x.Title).ToList();
                foreach (var title in expectedTitles)
                    Assert.Contains(title, fetchedTitles, StringComparer.OrdinalIgnoreCase);
            }
        }
    }
}
