using System.Threading.Tasks;
using Corax.Querying.Matches.Meta;
using FastTests;
using FastTests.Corax;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Corax;

// FastTests run a covering subset of these matrices
public class StreamingOptimization_RangeMatrix(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Indexes)]
    [MemberData(nameof(StreamingOptimization_QueryBuilder.RangesTests), true, MemberType = typeof(StreamingOptimization_QueryBuilder))]
    public async Task QueryBuilderRangeTests(bool hasMultipleValues, bool leftInclusive, bool rightInclusive, bool ascending) =>
        await StreamingOptimization_QueryBuilder.AssertRangeQuery(this, hasMultipleValues, leftInclusive, rightInclusive, ascending);

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Indexes)]
    [MemberData(nameof(StreamingOptimization_QueryBuilder.RangesTests), true, MemberType = typeof(StreamingOptimization_QueryBuilder))]
    public async Task QueryBuilderRangeTestsCannotApplyStreaming(bool hasMultipleValues, bool leftInclusive, bool rightInclusive, bool ascending) =>
        await StreamingOptimization_QueryBuilder.AssertRangeQueryCannotApplyStreaming(this, hasMultipleValues, leftInclusive, rightInclusive, ascending);

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Indexes)]
    [MemberData(nameof(StreamingOptimization_DataTests.RangesTests), true, MemberType = typeof(StreamingOptimization_DataTests))]
    public void DataRangeTests(bool leftInclusive, bool rightInclusive, bool ascending, OrderingType fieldType) =>
        StreamingOptimization_DataTests.AssertRangeQuery(this, leftInclusive, rightInclusive, ascending, fieldType);

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Indexes)]
    [MemberData(nameof(StreamingOptimization_DataTests.UnboundedRange), true, MemberType = typeof(StreamingOptimization_DataTests))]
    public void DataUnboundedRangeQueries(UnaryMatchOperation unaryMatchOperation, OrderingType fieldType, bool ascending, object value) =>
        StreamingOptimization_DataTests.AssertUnboundedRangeQuery(this, unaryMatchOperation, fieldType, ascending, value);
}
