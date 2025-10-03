using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Sparrow.Collections;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22253 : RavenTestBase
{
    public RavenDB_22253(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Voron)]
    public async Task QueryMustNotReturnNoResultsAfterWaitForIndexesAfterSaveChanges()
    {
        using var store = GetDocumentStore();

        store.ExecuteIndex(new PupilsIndex());

        var tasks = new List<Task>();
        var failures = new ConcurrentSet<string>();

        WaitForUserToContinueTheTest(store);

        // Run 10 concurrent tasks:
        for (int i = 0; i < 10; i++)
        {
            var taskId = i.ToString() + "-";

            tasks.Add(Task.Run(async () =>
            {
                // Add 1 pupil to each of 1000 schools:
                foreach (var schoolId in Enumerable
                    .Range(1, 100)
                    .Select(j => $"Schools/{taskId}{j}"))
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Pupil
                        {
                            SchoolId = schoolId,
                            FirstName = "John"
                        }, $"pupils/{schoolId}");
                        session.Advanced.WaitForIndexesAfterSaveChanges();
                        await session.SaveChangesAsync();
                    }

                    using (var session = store.OpenAsyncSession())
                    {
                        // Given the previous step uses WaitForIndexesAfterSaveChanges,
                        // we'd assume that the index contains the new pupil.
                        var pupils = await session.Query<Pupil, PupilsIndex>()
                            .Where(x => x.SchoolId == schoolId)
                            .ToListAsync();

                        if (pupils.Count == 0)
                        {
                            failures.Add($"No pupils found for {schoolId}");
                        }
                    }
                }
            }));
        }

        await Task.WhenAll(tasks);
        Assert.True(failures.Count == 0, string.Join('\n', failures));
    }

    class Pupil
    {
        public string Id { get; set; }
        public string SchoolId { get; set; }
        public string FirstName { get; set; }
    }

    class PupilsIndex : AbstractIndexCreationTask<Pupil>
    {

        public PupilsIndex()
        {
            Map = pupils => from pupil in pupils
                            select new
                            {
                                pupil.SchoolId
                            };
        }
    }
}
