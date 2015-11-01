using System.IO;
using Xunit;

namespace Voron.Tests.Trees
{
    public class CanIterateBackward : StorageTest
    {
        [PrefixesFact]
        public void SeekLastOnEmptyResultInFalse()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            using (var it = tx.Root.Iterate())
            {
                Assert.False(it.Seek(Slice.AfterAllKeys));

                tx.Commit();
            }
        }

        [PrefixesFact]
        public void CanSeekLast()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx.Root.Add("a", new MemoryStream(0));
                tx.Root.Add("c", new MemoryStream(0));
                tx.Root.Add("b", new MemoryStream(0));

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            using (var it = tx.Root.Iterate())
            {
                Assert.True(it.Seek(Slice.AfterAllKeys));
                Assert.Equal("c", it.CurrentKey.ToString());

                tx.Commit();
            }
        }

        [PrefixesFact]
        public void CanSeekBack()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx.Root.Add("a", new MemoryStream(0));
                tx.Root.Add("c", new MemoryStream(0));
                tx.Root.Add			("b", new MemoryStream(0));

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            using (var it = tx.Root.Iterate())
            {
                Assert.True(it.Seek(Slice.AfterAllKeys));
                Assert.Equal("c", it.CurrentKey.ToString());

                Assert.True(it.MovePrev());
                Assert.Equal("b", it.CurrentKey.ToString());

                Assert.True(it.MovePrev());
                Assert.Equal("a", it.CurrentKey.ToString());

                tx.Commit();
            }
        }
    }
}
