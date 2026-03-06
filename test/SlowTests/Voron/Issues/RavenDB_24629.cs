using FastTests.Voron;
using Sparrow.Platform;
using Tests.Infrastructure;
using Voron;
using Voron.Data.BTrees;
using Xunit;

namespace SlowTests.Voron.Issues;

public class RavenDB_24629 : StorageTest
{
    public RavenDB_24629(ITestOutputHelper output) : base(output)
    {
    }

    protected override void Configure(StorageEnvironmentOptions options)
    {
        options.ManualFlushing = true;
        options.Encryption.MasterKey = Sodium.GenerateRandomBuffer((int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes());
        options.MaxScratchBufferSize = 65536 - 1; // to make ShouldReduceSizeOfCompressionPager() return true
        options.Encryption.RegisterForJournalCompressionHandler();
    }
    
    [RavenFact(RavenTestCategory.Voron)]
    public void MustNotZeroMoreBytesThanCompressionBufferSize()
    {
        using (var tx = Env.WriteTransaction())
        {
            Tree tree = tx.CreateTree("foo");
            
            for (int i = 0; i < 100; i++)
            {
                tree.Add("items/" + i, new byte[1024 * 1024]);
            }
            
            tx.Commit();
        }
        
        Env.Journal.TryReduceSizeOfCompressionBufferIfNeeded();
        
        using (var tx = Env.WriteTransaction())
        {
            Env.Journal.ZeroCompressionBuffer(tx.LowLevelTransaction); // AVE before the fix
        }
    }
}
