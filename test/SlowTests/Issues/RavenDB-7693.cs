using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{

    public class RavenDB_7693 : RavenTestBase
    {
        public RavenDB_7693(ITestOutputHelper output) : base(output)
        {
        }

        private static string _projectTaskId;

        [Fact]
        public async Task SaveEntityWithEmptyListAfterLoad()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    var projectTask = new ProjectTask
                    {
                        Property1 = "1",
                        Property2 = "2",
                        Property3 = "3",
                        Property4 = "4"
                    };

                    await session.StoreAsync(projectTask);
                    await session.SaveChangesAsync();

                    _projectTaskId = projectTask.Id;
                }

                using (var session = store.OpenAsyncSession())
                {
                    await session.LoadAsync<ProjectTask>(_projectTaskId);
                    await session.SaveChangesAsync();
                }
            }
        }


        public class ProjectTask
        {
            public ProjectTask()
            {
                Issues = new List<string>();
            }

            public string Id { get; set; }
            public string Property1 { get; set; }
            public string Property2 { get; set; }
            public string Property3 { get; set; }
            public string Property4 { get; set; }
            public List<string> Issues { get; set; }
        }
    }
}
