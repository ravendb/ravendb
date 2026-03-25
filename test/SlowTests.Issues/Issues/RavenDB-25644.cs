using FastTests;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25644 : RavenTestBase
{
    public RavenDB_25644(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void AllInWithEmptyListShouldNotMatchAnything(Options options)
    {
        const string query = """
                              from Candidates as c
                              where c.frameworks all in ($frameworks)
                              """;
        
        string[] frameworks = [];

        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var candidate1 = new Candidate()
                {
                    Frameworks = [],
                    Languages = []
                };

                var candidate2 = new Candidate()
                {
                    Frameworks = [],
                    Languages = ["C#"]
                };

                var candidate3 = new Candidate()
                {
                    Frameworks = ["React"],
                    Languages = ["C#", "Java"]
                };
                
                 var candidate4 = new Candidate()
                 {
                     Frameworks = ["React", "Django"],
                     Languages = []
                 };
                 
                 var candidate5 = new Candidate()
                 {
                     Frameworks = ["React", "Django"], 
                     Languages = ["C#", "Java"]
                 };
                
                session.Store(candidate1);
                session.Store(candidate2);
                session.Store(candidate3);
                session.Store(candidate4);
                session.Store(candidate5);
                session.SaveChanges();
                
                var result = session.Advanced.RawQuery<Candidate>(query)
                    .AddParameter("frameworks", frameworks)
                    .WaitForNonStaleResults()
                    .ToList();
                
                Assert.Equal(0, result.Count);
            }
        }
    }

    private class Candidate
    {
        public string[] Frameworks { get; set; }
        public string[] Languages { get; set; }
    }
}
