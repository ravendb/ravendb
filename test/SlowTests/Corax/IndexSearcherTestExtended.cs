using System.Threading.Tasks;
using Corax.Querying;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Corax;

public class IndexSearcherTestExtended : NoDisposalNoOutputNeeded
{
    public IndexSearcherTestExtended(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [InlineData(100_000, 1028, BitmapAndFillMode.Off)]
    [InlineData(100_000, 2048, BitmapAndFillMode.Off)]
    [InlineData(100_000, 4096, BitmapAndFillMode.Off)]
    [InlineData(100_000, 1028, BitmapAndFillMode.Force)]
    [InlineData(100_000, 2048, BitmapAndFillMode.Force)]
    [InlineData(100_000, 4096, BitmapAndFillMode.Force)]
    public async Task MultiTermMatchWithBinaryOperations(int setSize, int stackSize, BitmapAndFillMode bitmapAndFillMode)
    {
        await using var testClass = new FastTests.Corax.IndexSearcherTest(Output);
        testClass.MultiTermMatchWithBinaryOperations(setSize, stackSize, bitmapAndFillMode);
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [InlineData(100000, 128, BitmapAndFillMode.Off)]
    [InlineData(100000, 2046, BitmapAndFillMode.Off)]
    [InlineData(11700, 18, BitmapAndFillMode.Off)]
    [InlineData(11859, 18, BitmapAndFillMode.Off)]
    [InlineData(100000, 128, BitmapAndFillMode.Force)]
    [InlineData(100000, 2046, BitmapAndFillMode.Force)]
    [InlineData(11700, 18, BitmapAndFillMode.Force)]
    [InlineData(11859, 18, BitmapAndFillMode.Force)]
    public async Task AndInStatementAndWhitespaceTokenizer(int setSize, int stackSize, BitmapAndFillMode bitmapAndFillMode)
    {
        await using var testClass = new FastTests.Corax.IndexSearcherTest(Output);
        testClass.AndInStatementAndWhitespaceTokenizer(setSize, stackSize, bitmapAndFillMode);
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [InlineData(100000, 2046, BitmapAndFillMode.Off)]
    [InlineData(11700, 18, BitmapAndFillMode.Off)]
    [InlineData(11859, 18, BitmapAndFillMode.Off)]
    [InlineData(100000, 2046, BitmapAndFillMode.Force)]
    [InlineData(11700, 18, BitmapAndFillMode.Force)]
    [InlineData(11859, 18, BitmapAndFillMode.Force)]
    public async Task AndInStatement(int setSize, int stackSize, BitmapAndFillMode bitmapAndFillMode)
    {
        await using var testClass = new FastTests.Corax.IndexSearcherTest(Output);
        testClass.AndInStatement(setSize, stackSize, bitmapAndFillMode);
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [InlineData(100000, 128, BitmapAndFillMode.Off)]
    [InlineData(100000, 18, BitmapAndFillMode.Off)]
    [InlineData(100000, 128, BitmapAndFillMode.Force)]
    [InlineData(100000, 18, BitmapAndFillMode.Force)]
    public async Task SimpleAndOrForBiggerSet(int setSize, int stackSize, BitmapAndFillMode bitmapAndFillMode)
    {
        await using var testClass = new FastTests.Corax.IndexSearcherTest(Output);
        testClass.SimpleAndOrForBiggerSet(setSize, stackSize, bitmapAndFillMode);
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [InlineData(100000, 128, BitmapAndFillMode.Off)]
    [InlineData(100000, 2046, BitmapAndFillMode.Off)]
    [InlineData(11700, 18, BitmapAndFillMode.Off)]
    [InlineData(11859, 18, BitmapAndFillMode.Off)]
    [InlineData(100000, 128, BitmapAndFillMode.Force)]
    [InlineData(100000, 2046, BitmapAndFillMode.Force)]
    [InlineData(11700, 18, BitmapAndFillMode.Force)]
    [InlineData(11859, 18, BitmapAndFillMode.Force)]
    public async Task AndInStatementWithLowercaseAnalyzer(int setSize, int stackSize, BitmapAndFillMode bitmapAndFillMode)
    {
        await using var testClass = new FastTests.Corax.IndexSearcherTest(Output);
        testClass.AndInStatementWithLowercaseAnalyzer(setSize, stackSize, bitmapAndFillMode);
    }
}
