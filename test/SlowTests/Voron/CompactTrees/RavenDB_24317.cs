using FastTests.Voron;
using Tests.Infrastructure;
using Voron.Data.CompactTrees;
using Voron.Impl;
using Xunit.Abstractions;

namespace SlowTests.Voron.CompactTrees;

public class RavenDB_24317(ITestOutputHelper output) : StorageTest(output)
{
    [RavenFact(RavenTestCategory.Voron)]
    public void CanSplitLastLeafWhenTermExistsOnlyAsLeafKey()
    {
        const string rootTreeName = nameof(RavenDB_24317);
        const string compactTreeName = nameof(CanSplitLastLeafWhenTermExistsOnlyAsLeafKey);


        using (var wTx = GetWriteTransaction(out var compactTree))
        {
            for (int i = 0; i < 1_000_000; ++i)
            {
                compactTree.Add(GetKey(i), i);
                if (i == 965_643)
                {
                    compactTree.TryRemove(GetKey(964_585), out _);
                }
            }
            
            wTx.Commit();
        }
        
        Transaction GetWriteTransaction(out CompactTree compactTree)
        {
            var wTx = Env.WriteTransaction();
            var rootTree = wTx.CreateTree(rootTreeName);
            compactTree = rootTree.CompactTreeFor(compactTreeName);
            return wTx;
        }
        
        string GetKey(int keyAsInt) => keyAsInt.ToString().PadLeft(totalWidth: 64, '0');
    }
}
