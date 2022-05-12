using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_18490 : RavenTestBase
{
    public RavenDB_18490(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void CanProvideMultiplePhrasesToSearchQuery()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                new CompanyCandidateSearchIndex().Execute(store);

                CompanyCandidateDocument entity = new CompanyCandidateDocument
                {
                    Id = "candidates/1",
                    FifoId = 1,
                    ProfileTitle = "Software Engineer",
                    CurrentRole = new JobExperience { RoleTitle = "Software Engineer" },
                    Specialties = { "Developing", "Matching" },
                    PreferredNextRoleNames = { "Team Lead" },
                    ActiveRoles = new List<JobExperience>
                    {
                        new JobExperience { RoleTitle = ".NET Framework Architect" },
                        new JobExperience { RoleTitle = "Developer of Software" },
                        new JobExperience { RoleTitle = "SQL Developer" },
                        new JobExperience { RoleTitle = "Should Match" }
                    },
                    MarketStatus = CandidateMarketStatus.InTheMarket
                };
                session.Store(entity);

                session.SaveChanges();

                Indexes.WaitForIndexing(store);

                var results = session.Query<CompanyCandidateSearchIndex.Result, CompanyCandidateSearchIndex>()
                    .Search(c => c.TextQuery, "\"Software Engineer\"", @operator: SearchOperator.And)
                    .Search(c => c.TextQuery, "\".NET Framework\"", options: SearchOptions.And, @operator: SearchOperator.And)
                    .OfType<CompanyCandidateDocument>()
                    .ToArray();

                Assert.Equal(1, results.Length);

                results = session.Query<CompanyCandidateSearchIndex.Result, CompanyCandidateSearchIndex>()
                    .Search(c => c.TextQuery, "\"Software Engineer\" \".NET Framework\"", @operator: SearchOperator.And)
                    .OfType<CompanyCandidateDocument>()
                    .ToArray();

                Assert.Equal(1, results.Length);
            }
        }
    }

    private class CompanyCandidateSearchIndex : AbstractIndexCreationTask<CompanyCandidateDocument>
    {
        public class Result
        {
            public object[] TextQuery { get; set; }
        }

        public CompanyCandidateSearchIndex()
        {
            Map = documents => from doc in documents
                               select new Result()
                               {
                                   TextQuery = new object[] { doc.ProfileTitle, doc.ActiveRoles.Select(x => new { x.RoleTitle }) }
                               };

            Index(nameof(Result.TextQuery), FieldIndexing.Search);
        }
    }

    private enum CandidateMarketStatus
    {
        InTheMarket
    }

    private class JobExperience
    {
        public string RoleTitle { get; set; }
    }

    private class CompanyCandidateDocument
    {
        public string Id { get; set; }
        public int FifoId { get; set; }
        public string ProfileTitle { get; set; }
        public JobExperience CurrentRole { get; set; }
        public List<string> Specialties { get; set; } = new();
        public List<string> PreferredNextRoleNames { get; set; } = new();
        public List<JobExperience> ActiveRoles { get; set; } = new();
        public CandidateMarketStatus MarketStatus { get; set; }
    }
}
