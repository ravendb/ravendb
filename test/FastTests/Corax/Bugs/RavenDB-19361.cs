using System.Threading.Tasks;
using FastTests.Voron;
using Voron.Data.Sets;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Bugs;

public class RavenDB_19361 : StorageTest
{
    public RavenDB_19361(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void EnsureProperNumberOfItemsInSetIsAccurate()
    {
        using (var wtx = Env.WriteTransaction())
        {
            var set = wtx.OpenSet("test");

            for (int i = 0; i < 3; i++)
            {
                for (int j = 0; j < SetLeafPage.MaxNumberOfRawValues +1; j++)
                {
                    set.Add(j + 1);
                }
            }
            wtx.Commit();
        }

        using (var rtx = Env.ReadTransaction())
        {
            var set = rtx.OpenSet("test");
            Assert.Equal(SetLeafPage.MaxNumberOfRawValues + 1, set.State.NumberOfEntries);
        }
    }
}
