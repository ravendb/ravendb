using System.Collections.Generic;
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
    
    [Fact]
    public void ProperRemovals()
    {
        var mem = new HashSet<long>();
        using (var wtx = Env.WriteTransaction())
        {
            var set = wtx.OpenSet("test");

            for (int j = 0; j < SetLeafPage.MaxNumberOfRawValues * 4; j++)
            {
                mem.Add(j + 1);
                set.Add(j + 1);
            }
            wtx.Commit();
        }
        
        using (var wtx = Env.WriteTransaction())
        {
            var set = wtx.OpenSet("test");

            set.Add(10_000);
            mem.Add(10_000);
            
            set.Remove(10_000);
            mem.Remove(10_000);
            set.Remove(20_000);
            mem.Remove(20_000);

            wtx.Commit();
        }


        using (var rtx = Env.ReadTransaction())
        {
            var set = rtx.OpenSet("test");
            Assert.Equal(mem.Count, set.State.NumberOfEntries);
        }
    }
}
