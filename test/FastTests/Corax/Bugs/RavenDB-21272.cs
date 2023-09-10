using System.Linq;
using System.Text;
using FastTests.Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Bugs;

public class RavenDB_21272 : StorageTest
{
    public RavenDB_21272(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void CanHandleDeletionOfSeparatorKeyWhenTheLeafKeyWasAlreadyRemoved()
    {
        const int Size = 2048;
        var keys = new CompactTree.CompactKeyLookup[Size];
        var vals = new long[Size];
        var offsets = new int[Size];

        int separatorIndex = -1;
        
        using (var wtx = Env.WriteTransaction())
        {
            var tree = wtx.CompactTreeFor("test");
            
            for (int i = 0; i < Size; i++)
            {
                keys[i].Key = new CompactKey();
                keys[i].Key.Initialize(wtx.LowLevelTransaction);
                keys[i].Key.Set(Encoding.UTF8.GetBytes(i.ToString("00000")));
                keys[i].Key.ChangeDictionary(tree.DictionaryId);
                keys[i].Key.EncodedWithCurrent(out _);
                keys[i].ContainerId = -1;
                vals[i] = 100000 + i;
            }

            // we create a tree that has mulitple pages

            var k = keys;
            var v = vals;
            var o = offsets;
            int index = 0;
            while (k.Length > 0)
            {
                int adjustment = 0;
                tree.InitializeStateForTryGetNextValue();
                var changed = tree.CheckTreeStructureChanges();
                var num = tree.BulkUpdateStart(k, v, o, out var page);
                int i = 0;
                for (; i < num; i++)
                {
                    index++;
                    tree.BulkUpdateSet(ref k[i], index + 10_000, page, o[i], ref adjustment);
                    if (changed.Changed)
                    {
                        for (int j = i; j < num; j++)
                        {
                            k[j].ContainerId = -1;
                        }
                        i++;
                        if (separatorIndex == -1)
                        {
                            separatorIndex = index;
                        }
                        break;
                    }
                }

                k = k[i..];
                v = v[i..];
                o = o[i..];
            }

            // we remove the entry that is the first key on the right most page
            //  root: [0...100], [101, 200]
            //      So we remove the value of 101, but the _separator_ remains 101
            tree.TryRemove(keys[separatorIndex], out _);

            // now we do a bulk operation that would:
            // first do a lookup on the key (which will load its container term id)
            // then delete _other values_, to trigger a page merge
            // as part of the page merge, we remove the separator key
            // and then we try to add the value again, which cause us to try to insert the key with
            // a term that was already removed
            k = keys.Skip(10).Take(separatorIndex-10).Concat(new[]{keys[separatorIndex]}).ToArray();
            for (int i = 0; i < k.Length; i++)
            {
                k[i].ContainerId = -1;
            }
            v = vals;
            o = offsets;
            while (k.Length > 0)
            {
                int adjustment = 0;
                tree.InitializeStateForTryGetNextValue();
                var changed = tree.CheckTreeStructureChanges();
                var num = tree.BulkUpdateStart(k, v, o, out var page);
                int i = 0;
                for (; i < num; i++)
                {
                    index++;
                    if (k[i].Key == keys[separatorIndex].Key)
                    {
                        tree.BulkUpdateSet(ref k[i], index + 10_000, page, o[i], ref adjustment);
                    }
                    else
                    {
                        tree.BulkUpdateRemove(ref k[i], page, o[i], ref adjustment, out _);
                    }
                    if (changed.Changed)
                    {
                        for (int j = i; j < num; j++)
                        {
                            k[j].ContainerId = -1;
                        }
                        break;
                    }
                }

                k = k[i..];
                v = v[i..];
                o = o[i..];
            }
            
            wtx.Commit();
        }
    }
}
