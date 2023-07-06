using System;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Corax;
using Corax.Mappings;
using Corax.Queries;
using Corax.Queries.SortingMatches;
using FastTests.Voron;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Json.Serialization.NewtonsoftJson.Internal;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Queries;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using Index = Raven.Server.Documents.Indexes.Index;

namespace FastTests.Corax;

public class StreamingOptimization(ITestOutputHelper output) : RavenTestBase(output)
{
    [Fact] // TermMatch and asc order on same field with same type => can optimize
    public async Task SortingMatchIsSkippedOnSingleTermMatch() => await TestQueryBuilder<TermMatch>(session => 
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndex>()
        .WhereEquals(p => p.Name, "maciej")
        .OrderBy(x => x.Name)
        .GetIndexQuery());
   
    [Fact] // TermMatch and desc order on same field with same type => can optimize
    public async Task SortingMatchIsNotSkippedOnSingleTermMatchDesc() => await TestQueryBuilder<TermMatch>(session => 
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndex>()
            .WhereEquals(p => p.Name, "maciej")
            .OrderByDescending(x => x.Name)
            .GetIndexQuery());

    [Fact] // TermMatch and asc order on same field with different type => cant optimize
    public async Task SortingMatchIsNotSkippedOnSingleTermMatchNumeric() => await TestQueryBuilder<SortingMatch>(session => 
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndex>()
            .WhereEquals(p => p.Name, "maciej")
            .OrderByDescending(x => x.Name, OrderingType.Long)
            .GetIndexQuery());

    [Fact] // TermMatch and desc order on same field with same type => can optimize
    public async Task SortingMatchIsSkippedWhenWeQueryOnTheSameType() => await TestQueryBuilder<TermMatch>(session => 
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndex>()
            .WhereEquals(p => p.First, (object)1L)
            .OrderBy(x => x.First, OrderingType.Long)
            .GetIndexQuery());
    
    [Fact] // TermMatch and desc order on same field with same type => can optimize
    public async Task SortingMatchIsSkippedWhenWeQueryOnTheSameType2() => await TestQueryBuilder<TermMatch>(session => 
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndex>()
            .WhereEquals(p => p.First, (object)1D)
            .OrderBy(x => x.First, OrderingType.Double)
            .GetIndexQuery());
    
    [Fact] //MultiTermMatch => cannot optimize
    public async Task SortingMatchIsNotSkippedOnMultiTermMatch() => await TestQueryBuilder<SortingMatch>(session => 
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndex>()
            .WhereLessThan(p => p.First, 10)
            .OrderBy(x => x.First)
            .GetIndexQuery());
    
    [Fact] // where Name = X and Field < 1 order by Name => where Name = x and Field < 1
    public async Task SortingMatchIsSkippedWhenIsAndBinaryMatch() => await TestQueryBuilder<BinaryMatch>(session => 
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndex>()
            .WhereEquals(p => p.Name, "maciej")
            .AndAlso()
            .WhereLessThan(p => p.First, 10)
            .OrderBy(x => x.Name)
            .GetIndexQuery());
    
    [Fact] // where Name = X or Field < 1 order by Name
    public async Task SortingMatchIsNotSkippedWhenIsOrBinaryMatch() => await TestQueryBuilder<SortingMatch>(session => 
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndex>()
            .WhereEquals(p => p.Name, "maciej")
            .OrElse()
            .WhereLessThan(p => p.First, 10)
            .OrderBy(x => x.Name)
            .GetIndexQuery());
    
    [Fact] // where (Name = x and F < 1) and (S = 2 and F < 2 ) order by Name => skip order by
    public async Task BinaryMatchOfBinaryMatchAnd() => await TestQueryBuilder<BinaryMatch>(session => 
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndex>()
                .OpenSubclause()
                    .WhereEquals(p => p.Name, "maciej")
                        .AndAlso()
                    .WhereLessThan(p => p.First, 10)
                .CloseSubclause()
            .AndAlso()
                .OpenSubclause()
                    .WhereEquals(p => p.Second, 2)
                        .AndAlso()
                    .WhereLessThan(p => p.First, 10)
                .CloseSubclause()
            .OrderBy(x => x.Name)
            .GetIndexQuery());
    
    [Fact] // where (Name = x and F < 1) and (S = 2 and F < 2 ) order by Name => skip order by
    public async Task BinaryMatchOfBinaryMatchOr() => await TestQueryBuilder<SortingMatch>(session => 
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndex>()
                .OpenSubclause()
                    .WhereEquals(p => p.Name, "maciej")
                        .AndAlso()
                    .WhereLessThan(p => p.First, 10)
                .CloseSubclause()
            .OrElse()
                .OpenSubclause()
                    .WhereEquals(p => p.Second, 2)
                        .AndAlso()
                    .WhereLessThan(p => p.First, 10)
                .CloseSubclause()
            .OrderBy(x => x.Name)
            .GetIndexQuery());
    
    [Fact] // where (Name = x or F < 1) and (S = 2 and F < 2 ) order by Name => cant skip order by
    public async Task BinaryOrMatchAndBinaryMatchOr() => await TestQueryBuilder<SortingMatch>(session => 
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndex>()
            .OpenSubclause()
            .WhereEquals(p => p.Name, "maciej")
            .OrElse()
            .WhereLessThan(p => p.First, 10)
            .CloseSubclause()
            .AndAlso()
            .OpenSubclause()
            .WhereEquals(p => p.Second, 2)
            .AndAlso()
            .WhereLessThan(p => p.First, 10)
            .CloseSubclause()
            .OrderBy(x => x.Name)
            .GetIndexQuery());

    [Fact] // where (Name = x and F < 1) and (S = 2 and F < 2 ) order by S => add sorting match
    public async Task BinaryMatchOfBinaryMatchAndButSortOnDifferentField() => await TestQueryBuilder<SortingMatch>(session => 
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndex>()
            .OpenSubclause()
            .WhereEquals(p => p.Name, "maciej")
            .AndAlso()
            .WhereLessThan(p => p.First, 10)
            .CloseSubclause()
            .AndAlso()
            .OpenSubclause()
            .WhereEquals(p => p.Second, 2)
            .AndAlso()
            .WhereLessThan(p => p.First, 10)
            .CloseSubclause()
            .OrderBy(x => x.Second)
            .GetIndexQuery());
    
    private async Task TestQueryBuilder<TExpected>(Func<IAsyncDocumentSession, IndexQuery> query)
    {
        var (store, index, mapping) = await GetDatabaseWithIndex();
        using var _ = store;
        using var context = JsonOperationContext.ShortTermSingleUse();
        var serializer = (JsonSerializer)store.Conventions.Serialization.CreateSerializer();

        {
            using var session = store.OpenAsyncSession();
            var coraxQuery = GetCoraxQuery(query(session), index, context, serializer, mapping);
            Assert.IsType<TExpected>(coraxQuery);
        }
    }

    private async Task<(DocumentStore store, Index index, IndexFieldsMapping mapping)> GetDatabaseWithIndex()
    {
        var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        var db = await GetDatabase(store.Database);
        var index = new DtoIndex();
        await index.ExecuteAsync(store);
        var indexInstance = db.IndexStore.GetIndex(index.IndexName);

        Type type = indexInstance.GetType();
        FieldInfo fieldInfo = type.GetField(nameof(Index.IndexPersistence), BindingFlags.NonPublic | BindingFlags.Instance);
        var cip = (CoraxIndexPersistence)fieldInfo?.GetValue(indexInstance);

        var cipType = cip.GetType();
        fieldInfo = cipType.GetField("_converter", BindingFlags.NonPublic | BindingFlags.Instance);
        var converter = (CoraxDocumentConverterBase)fieldInfo?.GetValue(cip);
        Assert.NotNull(converter); //name could change, adjust then

        return (store, indexInstance, converter?.GetKnownFieldsForWriter());
    }

    private record Dto(string Name, double First, double Second);

    private class DtoIndex : AbstractIndexCreationTask<Dto>
    {
        public DtoIndex()
        {
            Map = dtos => dtos.Select(e => new {e.Name, First = e.First, Second = e.Second});
        }
    }

    private class EnvTest(ITestOutputHelper output) : StorageTest(output);

    private IQueryMatch GetCoraxQuery(IndexQuery indexQuery, Index index, JsonOperationContext context, JsonSerializer jsonSerializer, IndexFieldsMapping mapping)
    {
        using var env = new EnvTest(Output);
        using (var writer = new BlittableJsonWriter(context))
        {
            jsonSerializer.Serialize(writer, indexQuery.QueryParameters);
            writer.FinalizeDocument();
            using var indexSearcher = new IndexSearcher(env.Env, mapping);
            using (var blittableParameters = writer.CreateReader())
            {
                var indexQueryServerSide = new IndexQueryServerSide(indexQuery.Query, blittableParameters);
                using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
                CoraxQueryBuilder.Parameters parameters = new(searcher: indexSearcher, bsc, null, null, indexQueryServerSide, index, blittableParameters, null, mapping,
                    null, null, int.MaxValue);
                var coraxQuery = CoraxQueryBuilder.BuildQuery(parameters, out _);

                return coraxQuery;
            }
        }
    }
}
