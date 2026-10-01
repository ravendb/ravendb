using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_22670 : RavenTestBase
    {
        public RavenDB_22670(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.ClientApi)]
        public void SearchOptionsMustNotEmitTheGroupOperatorInsideTheSubclause()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new Dto { Name = "test", Description = "the vision point" }, "dtos/1");
                session.Store(new Dto { Name = "test", Description = "the vision" }, "dtos/2");
                session.Store(new Dto { Name = "test", Description = "point sunset" }, "dtos/3");
                session.SaveChanges();
            }

            using var s = store.OpenSession();

            var cases = new (string Name, Func<IRavenQueryable<Dto>> Query, string Where)[]
            {
                ("ticket repro",
                    () => s.Query<Dto>().Where(x => x.Name == "test")
                        .Search(i => i.Description, "*ion")
                        .Search(i => i.Description, "point", options: SearchOptions.And | SearchOptions.Not)
                        .Search(i => i.Description, "*ion"),
                    "(Name = $p0) and search(Description, $p1) and (exists(Description) and not search(Description, $p2) or search(Description, $p3))"),

                ("And|Not then Guess",
                    () => s.Query<Dto>().Where(x => x.Name == "test")
                        .Search(i => i.Description, "point", options: SearchOptions.And | SearchOptions.Not)
                        .Search(i => i.Description, "sunset"),
                    "(Name = $p0) and ((exists(Description) and not search(Description, $p1)) or search(Description, $p2))"),

                ("And|Not on another field then Guess",
                    () => s.Query<Dto>().Where(x => x.Name == "test")
                        .Search(i => i.Description, "point", options: SearchOptions.And | SearchOptions.Not)
                        .Search(i => i.Name, "test"),
                    "(Name = $p0) and ((exists(Description) and not search(Description, $p1)) or search(Name, $p2))"),

                ("no where, Guess then And|Not then Guess",
                    () => s.Query<Dto>()
                        .Search(i => i.Description, "vision")
                        .Search(i => i.Description, "point", options: SearchOptions.And | SearchOptions.Not)
                        .Search(i => i.Description, "sunset"),
                    "search(Description, $p0) and (exists(Description) and not search(Description, $p1) or search(Description, $p2))"),

                ("where and one Guess search",
                    () => s.Query<Dto>().Where(x => x.Name == "test")
                        .Search(i => i.Description, "vision"),
                    "(Name = $p0) and search(Description, $p1)"),

                ("where and two Guess searches",
                    () => s.Query<Dto>().Where(x => x.Name == "test")
                        .Search(i => i.Description, "vision")
                        .Search(i => i.Description, "sunset"),
                    "(Name = $p0) and (search(Description, $p1) or search(Description, $p2))"),

                ("where and And",
                    () => s.Query<Dto>().Where(x => x.Name == "test")
                        .Search(i => i.Description, "vision", options: SearchOptions.And),
                    "(Name = $p0) and search(Description, $p1)"),

                ("where and And|Not",
                    () => s.Query<Dto>().Where(x => x.Name == "test")
                        .Search(i => i.Description, "point", options: SearchOptions.And | SearchOptions.Not),
                    "(Name = $p0) and (exists(Description) and not search(Description, $p1))"),

                ("where and Not",
                    () => s.Query<Dto>().Where(x => x.Name == "test")
                        .Search(i => i.Description, "point", options: SearchOptions.Not),
                    "(Name = $p0) and (exists(Description) and not search(Description, $p1))"),

                ("Guess then And|Not",
                    () => s.Query<Dto>().Where(x => x.Name == "test")
                        .Search(i => i.Description, "vision")
                        .Search(i => i.Description, "point", options: SearchOptions.And | SearchOptions.Not),
                    "(Name = $p0) and search(Description, $p1) and not search(Description, $p2)"),

                ("where and Or",
                    () => s.Query<Dto>().Where(x => x.Name == "test")
                        .Search(i => i.Description, "vision", options: SearchOptions.Or),
                    "(Name = $p0) and search(Description, $p1)"),

                ("search then where",
                    () => s.Query<Dto>()
                        .Search(i => i.Description, "vision")
                        .Where(x => x.Name == "test"),
                    "search(Description, $p0) and (Name = $p1)")
            };

            foreach (var (name, query, where) in cases)
            {
                var q = query();
                var expected = $"from 'Dtos' where {where}";
                var actual = q.ToString();

                Assert.True(expected == actual, $"{name}{Environment.NewLine}expected: {expected}{Environment.NewLine}actual:   {actual}");

                // the malformed RQL was rejected by the server, so the shapes must also be executable
                q.ToList();
            }

            var results = s.Query<Dto>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Name == "test")
                .Search(i => i.Description, "point", options: SearchOptions.And | SearchOptions.Not)
                .Search(i => i.Description, "sunset")
                .Select(x => x.Id)
                .ToList();

            Assert.Equal(new[] { "dtos/2", "dtos/3" }, results.OrderBy(x => x).ToArray());
        }

        private class Dto
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public string Description { get; set; }
        }
    }
}
