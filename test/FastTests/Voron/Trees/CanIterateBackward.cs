using System.IO;
using Xunit;

namespace Voron.Tests.Trees
{
    public class CanIterateBackward : StorageTest
    {
        [Fact]
        public void SeekLastOnEmptyResultInFalse()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                using (var it = tree.Iterate())
                {
                    Assert.False(it.Seek(Slice.AfterAllKeys));

                    tx.Commit();
                }
            }
        }

        [Fact]
        public void CanSeekLast()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tree.Add("a", new MemoryStream(0));
                tree.Add("c", new MemoryStream(0));
                tree.Add("b", new MemoryStream(0));

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                using (var it = tree.Iterate())
                {
                    Assert.True(it.Seek(Slice.AfterAllKeys));
                    Assert.Equal("c", it.CurrentKey.ToString());

                    tx.Commit();
                }
            }
        }

        [Fact]
        public void CanSeekBack()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tree.Add("a", new MemoryStream(0));
                tree.Add("c", new MemoryStream(0));
                tree.Add("b", new MemoryStream(0));

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("foo");
                using (var it = tree.Iterate())
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
}