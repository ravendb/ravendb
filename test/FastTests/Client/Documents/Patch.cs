﻿using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client.Documents
{
    public class Patch: RavenTestBase
    {
        public Patch(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.Patching)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void PatchOnEnumShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                string id;
                using (var session = store.OpenSession())
                {
                    var entity = new Job
                    {
                        Title = "Bulk insert",
                        Status = Status.Bad
                    };
                    session.Store(entity);
                    session.SaveChanges();
                    id = session.Advanced.GetDocumentId(entity);
                    
                }

                var expected = Status.Good;
                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<Job, Status>(id, x => x.Status, expected);
                    session.SaveChanges();
                    Assert.Equal(expected, session.Load<Job>(id).Status);
                }
            }
        }

        public class Job
        {
            public string Title { get; set; }
            public Status Status { get; set; }
        }

        public enum Status
        {
            None,
            Good,
            Bad
        }
    }
}
