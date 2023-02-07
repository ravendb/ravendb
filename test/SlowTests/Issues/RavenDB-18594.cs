using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_18594 : RavenTestBase
{
    public RavenDB_18594(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
    public void LuceneShouldApplyBoostingToDocumentInsteadOfIndividualFieldsInStaticIndex(Options options)
    {
        using var store = GetDocumentStore(options);
        {
            InsertUsers(store);
        }

        
        new SearchIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        {
            using var s = store.OpenSession();
            const string term = "Engineer";
               
            var query = s.Query<SearchIndex.ReduceResult, SearchIndex>().Search(r => r.ProfileTitle, term, boost: 10).Search(r => r.PreviousRolesSpecialties, term, boost: 5, SearchOptions.Or).OrderByScore();

            var docQuery = query.As<UserDocument>().ToDocumentQuery().IncludeExplanations(out var _);
            var queryResults = docQuery.GetQueryResult();
            var explanations = queryResults.Explanations;
            var results = query.As<UserDocument>().ToArray();
            Assert.Equal(2, results.Length);
            Assert.Equal(2, results.First().FifoId);
            Assert.Equal(1, results.Last().FifoId);
            Assert.Contains("0.875 = fieldNorm(field=PreviousRolesSpecialties, doc=0)", explanations["Doc1"][0]);
        }
    }
    
    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
    public void LuceneShouldApplyBoostingToDocumentInsteadOfIndividualFieldsInStaticJavaScriptIndex(Options options)
    {
        using var store = GetDocumentStore(options);
        {
            InsertUsers(store);
        }

        new SearchJavaScriptIndex().Execute(store);
        Indexes.WaitForIndexing(store);
        
        {
            using var s = store.OpenSession();
            const string term = "Engineer";
               
            var query = s.Query<SearchJavaScriptIndex.ReduceResult, SearchJavaScriptIndex>().Search(r => r.ProfileTitle, term, boost: 10).Search(r => r.PreviousRolesSpecialties, term, boost: 5, SearchOptions.Or).OrderByScore();

            var docQuery = query.As<UserDocument>().ToDocumentQuery().IncludeExplanations(out var _);
            var queryResults = docQuery.GetQueryResult();
            var explanations = queryResults.Explanations;
            var results = query.As<UserDocument>().ToArray();
            Assert.Equal(2, results.Length);
            Assert.Equal(2, results.First().FifoId);
            Assert.Equal(1, results.Last().FifoId);
            Assert.Contains("0.875 = fieldNorm(field=PreviousRolesSpecialties, doc=0)", explanations["Doc1"][0]);
        }
    }
    
    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CoraxShouldApplyBoostingToDocumentInsteadOfIndividualFieldsInStaticJavaScriptIndex(Options options)
    {
        using var store = GetDocumentStore(options);
        {
            InsertUsers(store);
        }

        new SearchJavaScriptIndex().Execute(store);
        Indexes.WaitForIndexing(store);
        {
            using var s = store.OpenSession();
            const string term = "Engineer";
               
            var query = s.Query<SearchJavaScriptIndex.ReduceResult, SearchJavaScriptIndex>().Search(r => r.ProfileTitle, term, boost: 10).Search(r => r.PreviousRolesSpecialties, term, boost: 5, SearchOptions.Or).OrderByScore();
            var results = query.As<UserDocument>().ToArray();
            Assert.Equal(2, results.Length);
            Assert.Equal(2, results.First().FifoId);
            Assert.Equal(1, results.Last().FifoId);
        }
    }

    private static void InsertUsers(DocumentStore store)
    {
        using var s = store.OpenSession();
        s.Store(new UserDocument
        {
            Id = "Doc1",
            FifoId = 1,
            CvRankingBoostFactor = 2,
            ProfileTitle = "Developer",
            CurrentRole = new ActiveRoles {FromDate = new DateTime(2012, 1, 1), ToDate = new DateTime(2020, 1, 1), Specialties = new List<string> {"Developer"}},
            ActiveRoles = new List<ActiveRoles>
            {
                new ActiveRoles {FromDate = new DateTime(2015, 1, 1), ToDate = new DateTime(2020, 1, 1), Specialties = new List<string> {"Engineer"}},
                new ActiveRoles {FromDate = new DateTime(2015, 1, 1), ToDate = new DateTime(2020, 1, 1), Specialties = new List<string> {"Coder"}},
                new ActiveRoles {FromDate = new DateTime(2015, 1, 1), ToDate = new DateTime(2020, 1, 1), Specialties = new List<string> {"Analyst"}},
                new ActiveRoles {FromDate = new DateTime(2015, 1, 1), ToDate = new DateTime(2020, 1, 1), Specialties = new List<string> {"Tester"}},
                new ActiveRoles {FromDate = new DateTime(2015, 1, 1), ToDate = new DateTime(2020, 1, 1), Specialties = new List<string> {"Dev"}}
            }
        });
        s.Store(new UserDocument
        {
            Id = "Doc2",
            FifoId = 2,
            CvRankingBoostFactor = 2,
            ProfileTitle = "Engineer",
            CurrentRole = new ActiveRoles {FromDate = new DateTime(2132, 1, 1), ToDate = new DateTime(2020, 1, 1), Specialties = new List<string> {"Developer"}},
            ActiveRoles = new List<ActiveRoles>
            {
                new ActiveRoles {FromDate = new DateTime(2015, 1, 1), ToDate = new DateTime(2020, 1, 1), Specialties = new List<string> {"Developer"}},
                new ActiveRoles {FromDate = new DateTime(2015, 1, 1), ToDate = new DateTime(2020, 1, 1), Specialties = new List<string> {"Coder"}},
                new ActiveRoles {FromDate = new DateTime(2015, 1, 1), ToDate = new DateTime(2020, 1, 1), Specialties = new List<string> {"Analyst"}},
                new ActiveRoles {FromDate = new DateTime(2015, 1, 1), ToDate = new DateTime(2020, 1, 1), Specialties = new List<string> {"Tester"}},
                new ActiveRoles {FromDate = new DateTime(2015, 1, 1), ToDate = new DateTime(2020, 1, 1), Specialties = new List<string> {"Dev"}}
            }
        });

        s.SaveChanges();
    }

    private class UserDocument
    {
        public List<ActiveRoles> ActiveRoles { get; set; }
        
        public ActiveRoles CurrentRole { get; set; }
        
        public int CvRankingBoostFactor { get; set; }
        
        public int FifoId { get; set; }
        
        public string ProfileTitle { get; set; }
        
        public string Id { get; set; }
    }
    
    private class ActiveRoles
    {
        public DateTimeOffset FromDate { get; set; }
        public List<string> Specialties { get; set; }
        public object ToDate { get; set; }
    }
    
    private class SearchIndex : AbstractIndexCreationTask<UserDocument, SearchIndex.ReduceResult>
    {
        public class ReduceResult
        {
            public string ProfileTitle { get; set; }
            public List<string> PreviousRolesSpecialties { get; set; }

            public int FifoId { get; set; }
        }

        public SearchIndex()
        {
            Map = docs => from doc in docs
                select new ReduceResult()
                {
                    PreviousRolesSpecialties = doc.ActiveRoles.Where(r4 => r4 != doc.CurrentRole).SelectMany(r => r.Specialties).ToList(),
                    FifoId = doc.FifoId,
                    ProfileTitle = doc.ProfileTitle
                    
                }.Boost(doc.CvRankingBoostFactor);
            
            Index(x => x.PreviousRolesSpecialties, FieldIndexing.Search);
            Index(x => x.ProfileTitle, FieldIndexing.Search);

            Analyze(x => x.PreviousRolesSpecialties, "StandardAnalyzer");
            Analyze(x => x.ProfileTitle, "StandardAnalyzer");
        }
    }
    
    private class SearchJavaScriptIndex : AbstractJavaScriptIndexCreationTask
    {
        public class ReduceResult
        {
            public string ProfileTitle { get; set; }
            public List<string> PreviousRolesSpecialties { get; set; }

            public int FifoId { get; set; }
        }

        public SearchJavaScriptIndex()
        {
            Maps = new HashSet<string>
            {
                @"map('UserDocuments', function (doc) { 
                    return boost({ 
                                    PreviousRolesSpecialties: doc.ActiveRoles.filter(r4 => r4 != doc.CurrentRole).map(r => r.Specialties).flat(), 
                                    FifoId: doc.FifoId, 
                                    ProfileTitle: doc.ProfileTitle
                                },
                                doc.CvRankingBoostFactor) })"
            };

            Fields = new Dictionary<string, IndexFieldOptions>()
            {
                {"PreviousRolesSpecialties", new IndexFieldOptions {Indexing = FieldIndexing.Search, Analyzer = "StandardAnalyzer"}},
                {"ProfileTitle", new IndexFieldOptions {Indexing = FieldIndexing.Search, Analyzer = "StandardAnalyzer"}},
            };
        }
    
    }

}
