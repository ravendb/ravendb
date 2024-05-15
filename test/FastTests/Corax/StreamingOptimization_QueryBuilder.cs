using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Corax;
using Corax.Indexing;
using Corax.Querying;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches;
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
using Voron;
using Voron.Data.BTrees;
using Xunit;
using Xunit.Abstractions;
using Index = Raven.Server.Documents.Indexes.Index;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace FastTests.Corax;

// In case when field has multiple terms per document we cannot optimize it for streaming so basically we've to assert two things:
// a) In case of single term per document -> assert if optimization happens (or not if its not valid)
// b) In case of multiple terms per document -> assert if optimization doesn't happened


public class StreamingOptimization_QueryBuilder : RavenTestBase
{
    public StreamingOptimization_QueryBuilder(ITestOutputHelper output) : base(output)
    {
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)] // TermMatch and asc order on same field with same type => can optimize
    public async Task SortingMatchIsSkippedOnSingleTermMatch(bool hasMultipleValues) => await TestQueryBuilder<TermMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
            .WhereEquals(p => p.Name, "maciej")
            .OrderBy(x => x.Name)
            .GetIndexQuery());

    [Theory]
    [InlineData(true)]
    [InlineData(false)] // TermMatch and desc order on same field with same type => can optimize
    public async Task SortingMatchIsNotSkippedOnSingleTermMatchDesc(bool hasMultipleValues) => await TestQueryBuilder<TermMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
            .WhereEquals(p => p.Name, "maciej")
            .OrderByDescending(x => x.Name)
            .GetIndexQuery());

    [Theory]
    [InlineData(true)]
    [InlineData(false)] // TermMatch and asc order on same field with different type => cant optimize
    public async Task SortingMatchIsNotSkippedOnSingleTermMatchNumeric(bool hasMultipleValues) => await TestQueryBuilder<SortingMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
            .WhereEquals(p => p.Name, "maciej")
            .OrderByDescending(x => x.Name, OrderingType.Long)
            .GetIndexQuery());

    [Theory]
    [InlineData(true)]
    [InlineData(false)] // TermMatch and desc order on same field with same type => can optimize
    public async Task SortingMatchIsSkippedWhenWeQueryOnTheSameType(bool hasMultipleValues) => await TestQueryBuilder<TermMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
            .WhereEquals(p => p.First, (object)1L)
            .OrderBy(x => x.First, OrderingType.Long)
            .GetIndexQuery());

    [Theory]
    [InlineData(true)]
    [InlineData(false)] // TermMatch and desc order on same field with same type => can optimize
    public async Task SortingMatchIsSkippedWhenWeQueryOnTheSameType2(bool hasMultipleValues) => await TestQueryBuilder<TermMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
            .WhereEquals(p => p.First, (object)1D)
            .OrderBy(x => x.First, OrderingType.Double)
            .GetIndexQuery());

    [Theory]
    [InlineData(true)]
    [InlineData(false)] // where Name = X and Field < 1 order by Name => where Name = x and Field < 1
    public async Task SortingMatchIsSkippedWhenIsAndBinaryMatch(bool hasMultipleValues) => await TestQueryBuilder<BinaryMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
            .WhereEquals(p => p.Name, "maciej")
            .AndAlso()
            .WhereLessThan(p => p.First, 10)
            .OrderBy(x => x.Name)
            .GetIndexQuery());

    [Theory]
    [InlineData(true)]
    [InlineData(false)] // where Name = X or Field < 1 order by Name
    public async Task SortingMatchIsNotSkippedWhenIsOrBinaryMatch(bool hasMultipleValues) => await TestQueryBuilder<SortingMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
            .WhereEquals(p => p.Name, "maciej")
            .OrElse()
            .WhereLessThan(p => p.First, 10)
            .OrderBy(x => x.Name)
            .GetIndexQuery());

    [Theory]
    [InlineData(true)]
    [InlineData(false)] // where (Name = x and F < 1) and (S = 2 and F < 2 ) order by Name => skip order by
    public async Task BinaryMatchOfBinaryMatchAnd(bool hasMultipleValues) => await TestQueryBuilder<BinaryMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
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

    [Theory]
    [InlineData(true)]
    [InlineData(false)] // where (Name = x and F < 1) or (S = 2 and F < 2 ) order by Name => skip order by
    public async Task BinaryMatchOfBinaryMatchOr(bool hasMultipleValues) => await TestQueryBuilder<SortingMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
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

    [Theory]
    [InlineData(true)]
    [InlineData(false)] // where (Name = x or F < 1) and (S = 2 and F < 2 ) order by Name => cant skip order by
    public async Task BinaryOrMatchAndBinaryMatchOr(bool hasMultipleValues) => await TestQueryBuilder<SortingMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
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

    [Theory]
    [InlineData(true)]
    [InlineData(false)] // where (Name = x and F < 1) and (S = 2 and F < 2 ) order by S => add sorting match
    public async Task BinaryMatchOfBinaryMatchAndButSortOnDifferentField(bool hasMultipleValues) => await TestQueryBuilder<SortingMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
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


    //DOUBLE SINGLE TESTS
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task LessThanOptimization(bool hasMultipleValues) => await TestQueryBuilder<MultiTermMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
            .WhereLessThan(p => p.First, 10)
            .OrderBy(x => x.First)
            .GetIndexQuery());

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task LessThanOptimizationWithTermMatch(bool hasMultipleValues) => await TestQueryBuilder<SortingMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
            .WhereLessThan(p => p.First, 10)
            .AndAlso()
            .WhereEquals(i => i.Name, "Maciej")
            .OrderBy(x => x.First)
            .GetIndexQuery());

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task LessThanOrEqualOptimization(bool hasMultipleValues) => await TestQueryBuilder<MultiTermMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
            .WhereLessThanOrEqual(p => p.First, 10)
            .OrderBy(x => x.First)
            .GetIndexQuery());

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task LessThanOrEqualOptimizationWithTermMatch(bool hasMultipleValues) => await TestQueryBuilder<SortingMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
            .WhereLessThanOrEqual(p => p.First, 10)
            .AndAlso()
            .WhereEquals(i => i.Name, "Maciej")
            .OrderBy(x => x.First)
            .GetIndexQuery());

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task GreaterThanOptimization(bool hasMultipleValues) => await TestQueryBuilder<MultiTermMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
            .WhereGreaterThan(p => p.First, 10)
            .OrderBy(x => x.First)
            .GetIndexQuery());

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task GreaterThanOptimizationWithTermMatch(bool hasMultipleValues) => await TestQueryBuilder<SortingMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
            .WhereGreaterThan(p => p.First, 10)
            .AndAlso()
            .WhereEquals(i => i.Name, "Maciej")
            .OrderBy(x => x.First)
            .GetIndexQuery());

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task GreaterThanOrEqualOptimization(bool hasMultipleValues) => await TestQueryBuilder<MultiTermMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
            .WhereGreaterThanOrEqual(p => p.First, 10)
            .OrderBy(x => x.First)
            .GetIndexQuery());

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task GreaterThanOrEqualOptimizationWithTermMatch(bool hasMultipleValues) => await TestQueryBuilder<SortingMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
            .WhereGreaterThanOrEqual(p => p.First, 10)
            .AndAlso()
            .WhereEquals(i => i.Name, "Maciej")
            .OrderBy(x => x.First)
            .GetIndexQuery());

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task StartsWithOptimization(bool hasMultipleValues) => await TestQueryBuilder<MultiTermMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
            .WhereStartsWith(p => p.Name, "mac")
            .OrderBy(x => x.Name)
            .GetIndexQuery());

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task StartsWithWithNumericOrderingD(bool hasMultipleValues) => await TestQueryBuilder<SortingMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
            .WhereStartsWith(p => p.Name, "mac")
            .OrderBy(x => x.Name, OrderingType.Double)
            .GetIndexQuery());

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task StartsWithWithNumericOrderingL(bool hasMultipleValues) => await TestQueryBuilder<SortingMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
            .WhereStartsWith(p => p.Name, "mac")
            .OrderBy(x => x.Name, OrderingType.Long)
            .GetIndexQuery());

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task StartsWithDifferentOrderByField(bool hasMultipleValues) => await TestQueryBuilder<SortingMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
            .WhereStartsWith(p => p.Name, "mac")
            .OrderBy(x => x.First)
            .GetIndexQuery());

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task StartsWithWithBinary(bool hasMultipleValues) => await TestQueryBuilder<SortingMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
            .WhereStartsWith(p => p.Name, "mac")
            .AndAlso()
            .WhereEquals(i => i.First, 10)
            .OrderBy(x => x.Name)
            .GetIndexQuery());


    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task EndsWithOptimization(bool hasMultipleValues) => await TestQueryBuilder<MultiTermMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
            .WhereEndsWith(p => p.Name, "mac")
            .OrderBy(x => x.Name)
            .GetIndexQuery());

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task EndsWithDifferentOrderByField(bool hasMultipleValues) => await TestQueryBuilder<SortingMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
            .WhereEndsWith(p => p.Name, "mac")
            .OrderBy(x => x.First)
            .GetIndexQuery());

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task EndsWithWithBinary(bool hasMultipleValues) => await TestQueryBuilder<SortingMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
            .WhereEndsWith(p => p.Name, "mac")
            .AndAlso()
            .WhereEquals(i => i.First, 10)
            .OrderBy(x => x.Name)
            .GetIndexQuery());

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ExistsOptimization(bool hasMultipleValues) => await TestQueryBuilder<MultiTermMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
            .WhereExists(p => p.Name)
            .OrderBy(x => x.Name)
            .GetIndexQuery());

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ExistsDifferentOrderByField(bool hasMultipleValues) => await TestQueryBuilder<SortingMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
            .WhereExists(p => p.Name)
            .OrderBy(x => x.First)
            .GetIndexQuery());

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ExistsWithBinary(bool hasMultipleValues) => await TestQueryBuilder<SortingMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
            .WhereExists(p => p.Name)
            .AndAlso()
            .WhereEquals(i => i.First, 10)
            .OrderBy(x => x.Name)
            .GetIndexQuery());

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task RegexOptimization(bool hasMultipleValues) => await TestQueryBuilder<MultiTermMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
            .WhereRegex(p => p.Name, "^[a-z ]{2,4}love")
            .OrderBy(x => x.Name)
            .GetIndexQuery());

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task RegexDifferentOrderByField(bool hasMultipleValues) => await TestQueryBuilder<SortingMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
            .WhereRegex(p => p.Name, "^[a-z ]{2,4}love")
            .OrderBy(x => x.First)
            .GetIndexQuery());

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task RegexWithBinary(bool hasMultipleValues) => await TestQueryBuilder<SortingMatch>(hasMultipleValues, session =>
        session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>()
            .WhereRegex(p => p.Name, "^[a-z ]{2,4}love")
            .AndAlso()
            .WhereEquals(i => i.First, 10)
            .OrderBy(x => x.Name)
            .GetIndexQuery());

    [Theory]
    [MemberData(nameof(RangesTests))]
    public async Task RangeTests(bool hasMultipleValues, bool leftInclusive, bool rightInclusive, bool ascending)
    {
        await TestQueryBuilder<MultiTermMatch>(hasMultipleValues, session =>
            {
                var query = session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>();

                query = leftInclusive
                    ? query.WhereGreaterThanOrEqual(i => i.First, -10D)
                    : query.WhereGreaterThan(i => i.First, -10D);

                query.AndAlso();

                query = rightInclusive
                    ? query.WhereLessThanOrEqual(i => i.First, 10D)
                    : query.WhereLessThan(i => i.First, 110D);

                query = ascending
                    ? query.OrderBy(i => i.First)
                    : query.OrderByDescending(i => i.First);

                return query.GetIndexQuery();
            }
        );
    }

    [Theory]
    [MemberData(nameof(RangesTests))]
    public async Task RangeTestsCannotApplyStreaming(bool hasMultipleValues, bool leftInclusive, bool rightInclusive, bool ascending)
    {
        await TestQueryBuilder<SortingMatch>(hasMultipleValues, session =>
            {
                var query = session.Advanced.AsyncDocumentQuery<Dto, DtoIndexSingleValues>();

                query = leftInclusive
                    ? query.WhereGreaterThanOrEqual(i => i.First, -10D)
                    : query.WhereGreaterThan(i => i.First, -10D);

                query.AndAlso();

                query = rightInclusive
                    ? query.WhereLessThanOrEqual(i => i.First, 10D)
                    : query.WhereLessThan(i => i.First, 110D);

                query.AndAlso().WhereEquals(i => i.Second, 0.0D);

                query = ascending
                    ? query.OrderBy(i => i.First)
                    : query.OrderByDescending(i => i.First);

                return query.GetIndexQuery();
            }
        );
    }

    public static IEnumerable<object[]> RangesTests()
    {
        var boolean = new bool[] { false, true };

        foreach (var hasMultipleValues in boolean)
            foreach (var leftInclusive in boolean)
                foreach (var rightInclusive in boolean)
                    foreach (var isAscending in boolean)
                    {
                        yield return new object[] { hasMultipleValues, leftInclusive, rightInclusive, isAscending };
                    }
    }

    private Task TestQueryBuilder<TExpectedForSingleValues>(bool hasMultipleValues, Func<IAsyncDocumentSession, IndexQuery> query)
    {
        return TestQueryBuilder<TExpectedForSingleValues, DtoIndexSingleValues>(this, hasMultipleValues, query);
    }

    public static async Task TestQueryBuilder<TExpectedForSingleValues, TIndex>(RavenTestBase self, bool hasMultipleValues, Func<IAsyncDocumentSession, IndexQuery> query)
        where TIndex : AbstractIndexCreationTask, new()
    {
        var (store, index, mapping, factories) = await GetDatabaseWithIndex<TIndex>(self);
        using var _ = store;
        using var context = JsonOperationContext.ShortTermSingleUse();
        var serializer = (JsonSerializer)store.Conventions.Serialization.CreateSerializer();
        {
            using var session = store.OpenAsyncSession();
            var coraxQuery = GetCoraxQuery(self, query(session), index, context, serializer, mapping, factories, hasMultipleValues);

            if (hasMultipleValues == false)
                Assert.IsType<TExpectedForSingleValues>(coraxQuery);
            else
                Assert.IsType<SortingMatch>(coraxQuery);
        }
    }

    private static async Task<(DocumentStore store, Index index, IndexFieldsMapping mapping, QueryBuilderFactories factories)> GetDatabaseWithIndex<TIndex>(RavenTestBase self)
        where TIndex : AbstractIndexCreationTask, new()
    {
        var store = self.GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        var db = await self.GetDatabase(store.Database);
        var index = new TIndex();
        await index.ExecuteAsync(store);
        var indexInstance = db.IndexStore.GetIndex(index.IndexName);

        Type type = indexInstance.GetType();
        FieldInfo fieldInfo = type.GetField(nameof(Index.IndexPersistence), BindingFlags.NonPublic | BindingFlags.Instance);
        var cip = (CoraxIndexPersistence)fieldInfo?.GetValue(indexInstance);

        fieldInfo = type.GetField("_queryBuilderFactories", BindingFlags.NonPublic | BindingFlags.Instance);
        var factories = (QueryBuilderFactories)fieldInfo?.GetValue(indexInstance);

        var cipType = cip.GetType();
        fieldInfo = cipType.GetField("_converter", BindingFlags.NonPublic | BindingFlags.Instance);
        var converter = (CoraxDocumentConverterBase)fieldInfo?.GetValue(cip);
        Assert.NotNull(converter); //name could change, adjust then

        return (store, indexInstance, converter?.GetKnownFieldsForWriter(), factories);
    }

    private record Dto(string Name, double First, double Second);

    private class DtoIndexSingleValues : AbstractIndexCreationTask<Dto>
    {
        public DtoIndexSingleValues()
        {
            Map = dtos => dtos.Select(e => new { e.Name, First = e.First, Second = e.Second });
        }
    }

    private class DtoIndexMultipleValues : AbstractIndexCreationTask<Dto>
    {
        public DtoIndexMultipleValues()
        {
            Map = dtos => dtos.Select(e => new
            {
                Name = new string[] { e.Name, e.Name.Reverse().ToString() },
                First = new double[] { e.First, 1.0 / e.First },
                Second = new double[] { e.Second, 1.0 / e.Second }
            });
        }
    }

    private class EnvTest : StorageTest
    {
        public EnvTest(ITestOutputHelper output) : base(output)
        {
        }

        //Since optimization use metadata made during indexing and it's extremely tricky to get transaction from RavenDB we make "fake" storage for it. sometimes we need to mark that we've multiple values
        //So let's add it directly ;-) When something will change about data structure remember to update this as well.
        public void Init(IndexFieldsMapping mapping, bool hasMultipleValues)
        {
            TransactionPersistentContext transactionPersistentContext = new(true);
            using (var transaction = Env.WriteTransaction(transactionPersistentContext))
            {
                using (var indexWriter = new IndexWriter(transaction, mapping, SupportedFeatures.All))
                {
                    const string someValue = "3.14";
                    
                    using (var builder = indexWriter.Index("entryKey"))
                    {
                        foreach (var field in mapping)
                        {
                            if (hasMultipleValues)
                                builder.IncrementList();

                            builder.Write(field.FieldId, Encoding.UTF8.GetBytes(someValue), 3, 3.14D);
                            
                            if (hasMultipleValues)
                            {
                                builder.Write(field.FieldId, Encoding.UTF8.GetBytes(someValue), 3, 3.14D);
                                builder.DecrementList();
                            }
                        }
                    }
                    indexWriter.Commit();
                }

                transaction.Commit();
            }
        }
    }

    private static IQueryMatch GetCoraxQuery(RavenTestBase self,
        IndexQuery indexQuery, Index index, JsonOperationContext context, JsonSerializer jsonSerializer, IndexFieldsMapping mapping,
        QueryBuilderFactories queryBuilderFactories, bool hasMultipleTermsInField)
    {
        using var env = new EnvTest(self.Output);
        env.Init(mapping, hasMultipleTermsInField);
        using (var writer = new BlittableJsonWriter(context))
        {
            jsonSerializer.Serialize(writer, indexQuery.QueryParameters);
            writer.FinalizeDocument();
            using var indexSearcher = new IndexSearcher(env.Env, mapping);
            using (var blittableParameters = writer.CreateReader())
            {
                var indexQueryServerSide = new IndexQueryServerSide(indexQuery.Query, blittableParameters);
                using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
                CoraxQueryBuilder.Parameters parameters = new(searcher: indexSearcher, bsc, null, null, indexQueryServerSide, index, blittableParameters,
                    queryBuilderFactories, mapping,
                    null, null, int.MaxValue);
                var coraxQuery = CoraxQueryBuilder.BuildQuery(parameters, out _);

                return coraxQuery;
            }
        }
    }
}
