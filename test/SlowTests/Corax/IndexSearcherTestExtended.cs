using System.Threading.Tasks;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Corax;

public class IndexSearcherTestExtended : NoDisposalNoOutputNeeded
{
    public IndexSearcherTestExtended(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [InlineData(100_000, 1028)]
    [InlineData(100_000, 2048)]
    [InlineData(100_000, 4096)]
    public async Task MultiTermMatchWithBinaryOperations(int setSize, int stackSize)
    {
        await using var testClass = new FastTests.Corax.IndexSearcherTest(Output);
        testClass.MultiTermMatchWithBinaryOperations(setSize, stackSize);
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [InlineData(new object[] {100000, 128})]
    [InlineData(new object[] {100000, 2046})]
    [InlineData(new object[] {11700, 18})]
    [InlineData(new object[] {11859, 18})]
    public async Task AndInStatementAndWhitespaceTokenizer(int setSize, int stackSize)
    {
        await using var testClass = new FastTests.Corax.IndexSearcherTest(Output);
        testClass.AndInStatementAndWhitespaceTokenizer(setSize, stackSize);
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [InlineData(new object[] {100000, 2046})]
    [InlineData(new object[] {11700, 18})]
    [InlineData(new object[] {11859, 18})]
    public async Task AndInStatement(int setSize, int stackSize)
    {
        await using var testClass = new FastTests.Corax.IndexSearcherTest(Output);
        testClass.AndInStatement(setSize, stackSize);
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [InlineData(new object[] {100000, 128})]
    [InlineData(new object[] {100000, 18})]
    public async Task SimpleAndOrForBiggerSet(int setSize, int stackSize)
    {
        await using var testClass = new FastTests.Corax.IndexSearcherTest(Output);
        testClass.SimpleAndOrForBiggerSet(setSize, stackSize);
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [InlineData(new object[] {100000, 128})]
    [InlineData(new object[] {100000, 2046})]
    [InlineData(new object[] {11700, 18})]
    [InlineData(new object[] {11859, 18})]
    public async Task AndInStatementWithLowercaseAnalyzer(int setSize, int stackSize)
    {
        await using var testClass = new FastTests.Corax.IndexSearcherTest(Output);
        testClass.AndInStatementWithLowercaseAnalyzer(setSize, stackSize);
    }
}
