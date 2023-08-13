using System.Collections.Generic;
using FastTests.Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

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
            var set = wtx.OpenPostingList("test");

            for (int i = 0; i < 3; i++)
            {
                for (int j = 0; j < 128 +1; j++)
                {
                    set.Add((j + 1) << 2);
                }
            }
            wtx.Commit();
        }

        using (var rtx = Env.ReadTransaction())
        {
            var set = rtx.OpenPostingList("test");
            Assert.Equal(128 + 1, set.State.NumberOfEntries);
        }
    }

    [Fact]
    public void ProperRemovals()
    {
        var mem = new HashSet<long>();
        using (var wtx = Env.WriteTransaction())
        {
            var set = wtx.OpenPostingList("test");

            for (int j = 0; j < 128 * 4; j++)
            {
                mem.Add((j + 1) << 2);
                set.Add((j + 1) << 2);
            }

            wtx.Commit();
        }

        using (var wtx = Env.WriteTransaction())
        {
            var set = wtx.OpenPostingList("test");

            set.Add(10_000 << 2);
            mem.Add(10_000 << 2);

            set.Remove(10_000 << 2);
            mem.Remove(10_000 << 2);
            set.Remove(20_000 << 2);
            mem.Remove(20_000 << 2);

            wtx.Commit();
        }


        using (var rtx = Env.ReadTransaction())
        {
            var set = rtx.OpenPostingList("test");
            Assert.Equal(mem.Count, set.State.NumberOfEntries);
        }
    }
}
