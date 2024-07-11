using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Documents.Queries.Timings;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;

public class RavenIntegration : RavenTestBase
{
    public RavenIntegration(ITestOutputHelper output) : base(output)
    {
    }


    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = new object[] { "from Docs where BoostFactor > 5.0 order by NonExist" })]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = new object[] { "from Docs where BoostFactor > 5.0 order by NonExist  as alphanumeric " })]
    public void FieldHasNoTerms(Options options, string query)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Doc() { Id = "1", BoostFactor = 10 });
            session.Store(new Doc() { Id = "2", BoostFactor = 20 });
            session.SaveChanges();

            var hasNoImpactOnOrder = session.Advanced.RawQuery<Doc>(query).WaitForNonStaleResults().ToList();
            Assert.Equal("1", hasNoImpactOnOrder[0].Id);
            Assert.Equal("2", hasNoImpactOnOrder[1].Id);
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void AutoIndexCanOrderByFieldThatExistsOnlyInSpecificDocument(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments();


        using var session = store.OpenSession();


        //In case of AutoIndexes this should work, in static index we can reproduce the issue with `CreateField` API
        var orderByClauseQuery = session.Advanced.RawQuery<object>("from TestCollection where Name = 'Maciej' order by OrderByClause").ToList();

        void InsertDocuments()
        {
            var requestExecutor = store.GetRequestExecutor();

            //Has OrderByClause
            using (requestExecutor.ContextPool.AllocateOperationContext(out var context))
            {
                var reader = context.ReadObject(new DynamicJsonValue()
                {
                    ["@metadata"] = new DynamicJsonValue() { ["@collection"] = "TestCollection" },
                    ["Name"] = "Maciej",
                    ["OrderByClause"] = "1"
                }, "num");
                requestExecutor.Execute(new PutDocumentCommand(requestExecutor.Conventions, "d1", null, reader), context);
            }

            //OrderByClause doesn't exist
            using (requestExecutor.ContextPool.AllocateOperationContext(out var context))
            {
                var reader = context.ReadObject(new DynamicJsonValue()
                {
                    ["@metadata"] = new DynamicJsonValue() { ["@collection"] = "TestCollection" },
                    ["Name"] = "Maciej",
                }, "num2");
                requestExecutor.Execute(new PutDocumentCommand(requestExecutor.Conventions, "d2", null, reader), context);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanIndexWithDocumentBoostAndDeleteTheItems(Options options)
    {
        using var store = GetDocumentStore(options);
        {
            using var session = store.OpenSession();
            session.Store(new Doc() { Name = "Two", BoostFactor = 1 });
            session.Store(new Doc() { Name = "Three", BoostFactor = 100 });
            session.Store(new Doc() { Name = "Four", BoostFactor = 200 });
            session.SaveChanges();
        }

        new DocIndex().Execute(store);
        Indexes.WaitForIndexing(store);
        {
            using var session = store.OpenSession();
            var results = session
                .Query<Doc, DocIndex>()
                .OrderByScore()
                .ToList();
            Assert.Equal(results.Count, 3);
            Assert.Equal(results[0].Name, "Four");
            Assert.Equal(results[1].Name, "Three");
            Assert.Equal(results[2].Name, "Two");
        }

        {
            using var session = store.OpenSession();
            var results = session
                .Query<Doc, DocIndex>()
                .ToList();
            Assert.Equal(results.Count, 3);
            Assert.Equal(results[0].Name, "Four");
            Assert.Equal(results[1].Name, "Three");
            Assert.Equal(results[2].Name, "Two");
        }


        {
            using var session = store.OpenSession();
            var doc = session.Query<Doc, DocIndex>().Single(i => i.Name == "Two");
            session.Delete(doc);
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);
        {
            using var session = store.OpenSession();
            var results = session.Query<Doc, DocIndex>().OrderByScore().ToList();
            Assert.Equal(results.Count, 2);
            Assert.Equal(results[0].Name, "Four");
            Assert.Equal(results[1].Name, "Three");
        }
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanUsePosititiveAndNegativeBoostFactors(Options options)
    {
        using var store = GetDocumentStore(options);
        {
            using var session = store.OpenSession();
            session.Store(new Doc() { Name = "Two", BoostFactor = -4 });
            session.Store(new Doc() { Name = "Three", BoostFactor = -3 });
            session.Store(new Doc() { Name = "Four", BoostFactor = -2 });
            session.Store(new Doc() { Name = "Five", BoostFactor = 5 });

            session.SaveChanges();
        }

        new DocIndex().Execute(store);
        Indexes.WaitForIndexing(store);
        {
            using var session = store.OpenSession();
            var results = session.Query<Doc, DocIndex>().OrderByScore().ToList();
            Assert.Equal(results.Count, 4);
            Assert.Equal(results[0].Name, "Five");
        }
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanIndexWithDocumentBoostAndUpdateTheItems(Options options)
    {
        using var store = GetDocumentStore(options);
        {
            using var session = store.OpenSession();
            session.Store(new Doc() { Name = "Two", BoostFactor = 2 });
            session.Store(new Doc() { Name = "Three", BoostFactor = 3 });
            session.Store(new Doc() { Name = "Four", BoostFactor = 4 });
            session.SaveChanges();
        }

        new DocIndex().Execute(store);
        Indexes.WaitForIndexing(store);
        {
            using var session = store.OpenSession();
            var results = session.Query<Doc, DocIndex>().OrderByScore().ToList();
            Assert.Equal(results.Count, 3);
            Assert.Equal(results[0].Name, "Four");
            Assert.Equal(results[1].Name, "Three");
            Assert.Equal(results[2].Name, "Two");
        }


        {
            using var session = store.OpenSession();
            var doc = session.Query<Doc, DocIndex>().Single(i => i.Name == "Two");
            doc.BoostFactor = 5;
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);
        {
            using var session = store.OpenSession();
            var results = session.Query<Doc, DocIndex>().OrderByScore().ToList();
            Assert.Equal(results.Count, 3);
            Assert.Equal(results[0].Name, "Two");
            Assert.Equal(results[1].Name, "Four");
            Assert.Equal(results[2].Name, "Three");
        }
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void IndexTimeDocumentBoostViaLinq(Options options) => IndexTimeDocumentBoost<DocIndex>(options);

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void IndexTimeDocumentBoostViaJs(Options options) => IndexTimeDocumentBoost<JsDocIndex>(options);

    private void IndexTimeDocumentBoost<T>(Options options, IDocumentStore defaultStore = null) where T : AbstractIndexCreationTask, new()
    {
        using var store = GetDocumentStore(options);
        {
            using var session = store.OpenSession();
            session.Store(new Doc() { Name = "Two", BoostFactor = 2 });
            session.Store(new Doc() { Name = "Three", BoostFactor = 3 });
            session.Store(new Doc() { Name = "Four", BoostFactor = 4 });
            session.SaveChanges();
        }

        new T().Execute(store);
        Indexes.WaitForIndexing(store);
        {
            using var session = store.OpenSession();
            var results = session.Query<Doc, T>().OrderByScore().ToList();
            Assert.Equal(results.Count, 3);
            Assert.Equal(results[0].Name, "Four");
            Assert.Equal(results[1].Name, "Three");
            Assert.Equal(results[2].Name, "Two");
        }
    }

    private class DocIndex : AbstractIndexCreationTask<Doc>
    {
        public DocIndex()
        {
            Map = docs => from doc in docs
                          select new { Name = doc.Name }.Boost(doc.BoostFactor);
        }
    }

    private class JsDocIndex : AbstractJavaScriptIndexCreationTask
    {
        public JsDocIndex()
        {
            Maps = new HashSet<string> { @"map('Docs', function (u){ return boost({ Name: u.Name}, u.BoostFactor);})", };
        }
    }

    private class Doc
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public float BoostFactor { get; set; }
    }


    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanMixValuesInQueryForBetween(Options options)
    {
        using var store = GetDocumentStore(options);
        {
            using var s = store.OpenSession();
            s.Store(new DoubleItem(2));
            s.SaveChanges();
        }

        {
            using var s = store.OpenSession();
            var q = s.Advanced.RawQuery<DoubleItem>("from DoubleItems where 'Value' < 3 and 'Value' > 1.5").ToList();
            Assert.Equal(1, q.Count);
        }
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanGetQueryPlanViaQuery(Options options)
    {
        using var store = GetDocumentStore(options);
        {
            using var s = store.OpenSession();
            s.Store(new DoubleItem(2));
            s.SaveChanges();
        }

        {
            using var s = store.OpenSession();
            QueryTimings timings = null;
            var q = s.Query<DoubleItem>()
                .Customize(x =>
                    x
                        .WaitForNonStaleResults()
                        .Timings(out timings))
                .Where(x => x.Value > 1D)
                .ToList();
            Assert.Equal(1, q.Count);
            Assert.IsType<QueryInspectionNode>(timings.QueryPlan);
        }
    }

    private record DoubleItem(double Value);

    [RavenTheory(RavenTestCategory.Facets)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    public void CanCreateFacetsOnDynamicFields(Options options)
    {
        using var store = DatabaseForDynamicIndex(options);

        {
            using var session = store.OpenSession();
            var results = session.Query<DtoForDynamics, DynamicIndex>().AggregateBy(builder => builder.ByField("DynamicTag")).Execute();
            Assert.Equal(1, results.Count);
            var tagFacets = results["DynamicTag"].Values;
            var coraxTag = tagFacets.Single(i => i.Range == "corax");
            Assert.Equal(2, coraxTag.Count);
            var restTags = tagFacets.Where(i => i.Range != "corax").ToArray();
            Assert.True(restTags.Select(i => i.Range).Contains("rachis"));
            Assert.True(restTags.Select(i => i.Range).Contains("lucene"));
            Assert.True(restTags.Select(i => i.Range).Contains("sparrow"));
            Assert.DoesNotContain(restTags, x => x.Count != 1);
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanUseMoreLikeThisForDynamicFields(Options options)
    {
        using var store = DatabaseForDynamicIndex(options);
        {
            using var session = store.OpenSession();
            var mlt = session.Query<DtoForDynamics, DynamicIndex>().MoreLikeThis(builder =>
                builder.UsingDocument(@"{""DynamicTag"": ""Corax""}").WithOptions(new MoreLikeThisOptions { Fields = new[] { "DynamicTag" }, })).ToList();
            Assert.Equal(2, mlt.Count);
        }
    }

    private IDocumentStore DatabaseForDynamicIndex(Options options)
    {
        var store = GetDocumentStore(options);
        {
            using var session = store.OpenSession();
            session.Store(new DtoForDynamics() { Tag = "Corax" });
            session.Store(new DtoForDynamics() { Tag = "Corax" });
            session.Store(new DtoForDynamics() { Tag = "Lucene" });
            session.Store(new DtoForDynamics() { Tag = "Rachis" });
            session.Store(new DtoForDynamics() { Tag = "Sparrow" });
            session.SaveChanges();
        }

        var index = new DynamicIndex();
        index.Execute(store);
        Indexes.WaitForIndexing(store);

        return store;
    }

    private class DynamicIndex : AbstractIndexCreationTask<DtoForDynamics>
    {
        public DynamicIndex()
        {
            Map = dtos => dtos.Select(i => new { _ = CreateField("DynamicTag", i.Tag) });
        }
    }

    private class DtoForDynamics
    {
        public string Id { get; set; }
        public string Tag { get; set; }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CoraxOrQueriesCanWorksOnComplexQueries(Options options)
    {
        using var store = GetDocumentStore(options);
        {
            using var session = store.OpenSession();
            session.Store(new Person("Maciej"));
            session.SaveChanges();
        }

        {
            using var session = store.OpenSession();
            var person = session.Advanced.DocumentQuery<Person>()
                .OpenSubclause()
                .WhereStartsWith(i => i.Name, "mac")
                .OrElse()
                .WhereEndsWith(i => i.Name, "iej")
                .CloseSubclause()
                .Boost(10)
                .Single();
            Assert.Equal("Maciej", person.Name);
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CoraxWillSortByScoreAutomaticallyWhenQueryHasBoosting(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Person("Maciej"));
            session.Store(new Person("Marika"));
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            var resultsNoBoosting = session.Query<Person>()
                .ToDocumentQuery()
                .WaitForNonStaleResults()
                .WhereEquals(i => i.Name, "Maciej")
                .OrElse()
                .WhereEquals(i => i.Name, "marika")
                .ToList();

            var resultWithBoosting = session.Query<Person>()
                .ToDocumentQuery()
                .WaitForNonStaleResults()
                .WhereEquals(i => i.Name, "Maciej").Boost(1)
                .OrElse()
                .WhereEquals(i => i.Name, "marika").Boost(1000)
                .ToList();

            Assert.Equal(resultsNoBoosting[0].Name, resultWithBoosting[1].Name);
            Assert.Equal(resultsNoBoosting[1].Name, resultWithBoosting[0].Name);
        }
    }

    private record Person(string Name);

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanQueryWithLongOnDoubleField(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Doc { Name = "Maciej", BoostFactor = 11.5f });
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            //  The `11` on the server side will be long ( and the string made of it is "11") but the index doesn't contain the such term (because we indexed it as `11.5`.)
            var query = session.Advanced.RawQuery<Doc>("from Docs where BoostFactor == 11").WaitForNonStaleResults().ToList();
            WaitForUserToContinueTheTest(store);
            Assert.Equal(1, query.Count);
        }
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanUpdateSingleWhenFrequencyIsChangingLevel(Options options)
    {
        const string initial = "12345";
        string DataGenerator(int i) => string.Join(" ", Enumerable.Range(0, i).Select(i => initial));
        using var store = GetDocumentStore(options);
        DtoForDynamics docToModify;
        using (var session = store.OpenSession())
        {
            docToModify = new DtoForDynamics() { Tag = DataGenerator(17) };
            session.Store(docToModify);
            session.SaveChanges();
        }

        var index = new SearchIndex();
        index.Execute(store);
        Indexes.WaitForIndexing(store);
        using (var session = store.OpenSession())
        {
            var doc = session.Load<DtoForDynamics>(docToModify.Id);
            doc.Tag = DataGenerator(1);
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);
        WaitForUserToContinueTheTest(store);
    }

    private class SearchIndex : AbstractIndexCreationTask<DtoForDynamics>
    {
        public SearchIndex()
        {
            Map = enumerable => enumerable.Select(i => new { i.Tag });
            Index(i => i.Tag, FieldIndexing.Search);
        }
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void TermMatchCanQueryOnDoubleTermThatDoesntExists(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Doc { Name = "Maciej", BoostFactor = 11.5f });
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            var results = session.Query<Doc>().Where(i => i.BoostFactor == 0f).ToList();
            Assert.Empty(results);
        }
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanDistinguishBetweenIdMethodAndIdFieldName(Options options)
    {
        using var store = GetDocumentStore(options);
        var idObject = new IdClass();
        using (var session = store.OpenSession())
        {
            session.Store(idObject);
            session.SaveChanges();
        }

        var index = new IdIndex();
        index.Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var classicId = session.Advanced.RawQuery<IdClass>($"from index '{index.IndexName}' where id() == '{idObject.Id}'").Count();
            var modifiedId = session.Advanced.RawQuery<IdClass>($"from index '{index.IndexName}' where startsWith(Id, 'modified')").Count();
            Assert.Equal(1, classicId);
            Assert.Equal(1, modifiedId);
        }
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanDistinguishBetweenIdMethodAndIdFieldName2(Options options)
    {
        using var store = GetDocumentStore(options);
        var doc = new Doc() { Name = "maciej" };
        using (var session = store.OpenSession())
        {
            session.Store(doc);
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            var classicId = session.Advanced.RawQuery<IdClass>($"from Docs where Name == 'maciej' and  id() == '{doc.Id}'").WaitForNonStaleResults().Count();
            WaitForUserToContinueTheTest(store);
            var classicIdViaFieldName = session.Advanced.RawQuery<IdClass>($"from Docs where Name == 'maciej' and Id == '{doc.Id}'").WaitForNonStaleResults().Count();
            Assert.Equal(1, classicId);
            Assert.Equal(0, classicIdViaFieldName);
        }
    }


    private class IdClass
    {
        public string Id { get; set; }
    }

    private class IdIndex : AbstractIndexCreationTask<IdClass>
    {
        public IdIndex()
        {
            Map = classes => classes.Select(i => new { Id = "ModifiedId" + i.Id });
        }
    }

    private class User
    {
        public string Id { get; set; }
        public string Name { get; set; }

        public Dictionary<string, string> Dict { get; set; }
    }


    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanUpdateDynamicFields(Options options)
    {
        using var store = GetDocumentStore(options);
        var user = new User() { Name = "TestDoc", Dict = new Dictionary<string, string>() { { "secret_field", "maciej" } } };
        using (var session = store.OpenSession())
        {
            session.Store(user);
            session.SaveChanges();
        }

        var index = new DynamicFieldIndex();
        index.Execute(store);
        Indexes.WaitForIndexing(store);
        WaitForUserToContinueTheTest(store);

        using (var session = store.OpenSession())
        {
            var count = session.Query<User, DynamicFieldIndex>().ToDocumentQuery().WhereEquals("secret_field", "maciej").Count();
            Assert.Equal(1, count);

            var loadDoc = session.Load<User>(user.Id);
            loadDoc.Dict["secret_field"] = "jan";
            session.Store(loadDoc);
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var count = session.Query<User, DynamicFieldIndex>().ToDocumentQuery().WhereEquals("secret_field", "maciej").Count();
            Assert.Equal(0, count);
            count = session.Query<User, DynamicFieldIndex>().ToDocumentQuery().WhereEquals("secret_field", "jan").Count();
            Assert.Equal(1, count);
        }
    }

    private class DynamicFieldIndex : AbstractIndexCreationTask<User>
    {
        public DynamicFieldIndex()
        {
            Map = users => users.Select(u => new { _ = u.Dict.Select(d => CreateField(d.Key, d.Value)) });
        }
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, Data = new object[] { 2 })]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, Data = new object[] { 10 })]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, Data = new object[] { 256 })]
    public async Task CanUpdateFanout(Options options, int count)
    {
        using var store = GetDocumentStore(options);
        var doc = new FanoutDto() { Data = Enumerable.Range(0, count).Select(i => i).ToList() };
        var index = new FanoutIndex();
        await index.ExecuteAsync(store);

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(doc);
            await session.SaveChangesAsync();
        }

        await Indexes.WaitForIndexingAsync(store);


        long entriesCount = await AssertWaitForValueAsync(async () => (int)(await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(index.IndexName))).EntriesCount, count);
        Assert.Equal(count, entriesCount);

        using (var session = store.OpenAsyncSession())
        {
            doc = await session.LoadAsync<FanoutDto>(doc.Id);
            doc.Data = doc.Data.Take(count / 2).ToList(); // remove half

            await session.StoreAsync(doc);
            await session.SaveChangesAsync();
        }

        await Indexes.WaitForIndexingAsync(store);
        entriesCount = await AssertWaitForValueAsync(async () => (int)(await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(index.IndexName))).EntriesCount, count / 2);
        Assert.Equal(count / 2, entriesCount);
    }

    [Fact]
    public void AllInUnaryMatchWillDetectEachDocumentSeparately()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var session = store.OpenSession();
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
        session.Store(new FanoutDto()
        {
            Data = new() { 1, 2, 3, 4, 5 }
        });
        session.SaveChanges();
        session.Store(new FanoutDto() { Data = new() { 1, 2, 3, 4 } });
        session.Store(new FanoutDto() { Data = new() { 1, 2, 3, 4 } });
        session.SaveChanges();

        var result = session.Advanced.DocumentQuery<FanoutDto>().ContainsAll(i => i.Data, new[] { 1, 2, 3, 4, 5 }).ToList();
        Assert.Equal(1, result.Count);
    }

    private class FanoutIndex : AbstractIndexCreationTask<FanoutDto>
    {
        public FanoutIndex()
        {
            Map = dtos => from doc in dtos
                          from num in doc.Data
                          select new { Number = num };
        }
    }

    private class FanoutDto
    {
        public string Id { get; set; }
        public int Number { get; set; }
        public List<int> Data { get; set; }
    }
}
