using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public class IndexSearcherTestExtended : NoDisposalNoOutputNeeded
{
    public IndexSearcherTestExtended(ITestOutputHelper output) : base(output)
    {
    }

    [Theory]
    [InlineData(100_000, 1028)]
    [InlineData(100_000, 2048)]
    [InlineData(100_000, 4096)]
    public void MultiTermMatchWithBinaryOperations(int setSize, int stackSize)
    {
        using var testClass = new FastTests.Corax.IndexSearcherTest(Output);
        testClass.MultiTermMatchWithBinaryOperations(setSize, stackSize);
    }

    [Theory]
    [InlineData(new object[] {100000, 128})]
    [InlineData(new object[] {100000, 2046})]
    [InlineData(new object[] {11700, 18})]
    [InlineData(new object[] {11859, 18})]
    public void AndInStatementAndWhitespaceTokenizer(int setSize, int stackSize)
    {
        using var testClass = new FastTests.Corax.IndexSearcherTest(Output);
        testClass.AndInStatementAndWhitespaceTokenizer(setSize, stackSize);
    }

    [InlineData(new object[] {100000, 2046})]
    [InlineData(new object[] {11700, 18})]
    [InlineData(new object[] {11859, 18})]
    public void AndInStatement(int setSize, int stackSize)
    {
        using var testClass = new FastTests.Corax.IndexSearcherTest(Output);
        testClass.AndInStatement(setSize, stackSize);
    }

    [Theory]
    [InlineData(new object[] {100000, 128})]
    [InlineData(new object[] {100000, 18})]
    public void SimpleAndOrForBiggerSet(int setSize, int stackSize)
    {
        using var testClass = new FastTests.Corax.IndexSearcherTest(Output);
        testClass.SimpleAndOrForBiggerSet(setSize, stackSize);
    }

    [Theory]
    [InlineData(new object[] {100000, 128})]
    [InlineData(new object[] {100000, 2046})]
    [InlineData(new object[] {11700, 18})]
    [InlineData(new object[] {11859, 18})]
    public void AndInStatementWithLowercaseAnalyzer(int setSize, int stackSize)
    {
        using var testClass = new FastTests.Corax.IndexSearcherTest(Output);
        testClass.AndInStatementWithLowercaseAnalyzer(setSize, stackSize);
    }
}
