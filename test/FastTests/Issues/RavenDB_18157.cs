using System.Linq;
using Raven.Server.Documents.Indexes.Static;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues;

public class RavenDB_18157 : RavenTestBase
{
    public RavenDB_18157(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void Can_Use_Indexer_In_DynamicArray()
    {
        var dynamicArray = new DynamicArray(Enumerable.Range(0, 5));

        var element = dynamicArray[1]; // does not work

        Assert.Equal(1, element);

        dynamic d = dynamicArray;

        element = d[1]; // works;

        Assert.Equal(1, element);
    }
}
