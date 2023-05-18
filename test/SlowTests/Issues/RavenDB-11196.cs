using System;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Tests.Infrastructure.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11196 : RavenTestBase
    {
        public RavenDB_11196(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Should_be_James(Options options)
        {
            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore())
            {
                Etl.AddEtl(src, dest, "Users", script:
                    @"
this.Name = 'James';

loadToUsers(this);

var person = { Name: this.Name };

loadToPeople(person);
"
                );

                var etlDone = Etl.WaitForEtlToComplete(src);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe",
                        LastName = "Doe",
                    }, "users/1");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(100));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    Assert.NotNull(user);
                    Assert.Equal("James", user.Name);

                    var person = session.Advanced.LoadStartingWith<Person>("users/1/people/")[0];

                    Assert.NotNull(person);
                    Assert.Equal("James", person.Name); // throws here, actual: Joe
                }
            }
        }

        [RavenTheory(RavenTestCategory.Patching)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CanDeleteEverything(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                Indexes.WaitForIndexing(store);

                var operation = store.Operations.Send(new PatchByQueryOperation("from @all_docs as doc update {  del(id(doc)); }"));
                operation.WaitForCompletion(TimeSpan.FromMinutes(5));

                var tester = store.Maintenance.ForTesting(() => new GetStatisticsOperation());

                tester.AssertAll((_, stats) =>
                {
                    Assert.True(stats.CountOfDocuments <= 8, $"stats.CountOfDocuments: {stats.CountOfDocuments}"); // hi-lo
                });
            }
        }
    }
}
