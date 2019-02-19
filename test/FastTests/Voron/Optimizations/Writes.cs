// -----------------------------------------------------------------------
//  <copyright file="Foo.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using Xunit;
using Voron.Debugging;

namespace FastTests.Voron.Optimizations
{
    public class Writes : StorageTest
    {
        [Fact]
        public void SinglePageModificationDoNotCauseCopyingAllIntermediatePages()
        {
            var keySize = 1024;
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tree.Add(new string('9', keySize), new MemoryStream(new byte[3]));
                DebugStuff.RenderAndShow(tree);
                tree.Add(new string('1', keySize), new MemoryStream(new byte[3]));
                DebugStuff.RenderAndShow(tree);
                tree.Add(new string('4', 1000), new MemoryStream(new byte[2]));
                DebugStuff.RenderAndShow(tree);
                tree.Add(new string('5', keySize), new MemoryStream(new byte[2]));
                DebugStuff.RenderAndShow(tree);
                tree.Add(new string('8', keySize), new MemoryStream(new byte[3]));
                DebugStuff.RenderAndShow(tree);
                tree.Add(new string('2', keySize), new MemoryStream(new byte[2]));
                DebugStuff.RenderAndShow(tree);
                tree.Add(new string('6', keySize), new MemoryStream(new byte[2]));
                DebugStuff.RenderAndShow(tree);
                tree.Add(new string('0', keySize), new MemoryStream(new byte[4]));
                DebugStuff.RenderAndShow(tree);
                tree.Add(new string('3', 1000), new MemoryStream(new byte[1]));
                DebugStuff.RenderAndShow(tree);
                tree.Add(new string('7', keySize), new MemoryStream(new byte[1]));
                
                tx.Commit();
            }

            var afterAdds = Env.NextPageNumber;

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tree.Delete(new string('0', keySize));

                tree.Add(new string('4', 1000), new MemoryStream(new byte[21]));

                tx.Commit();
            }

            Assert.Equal(afterAdds, Env.NextPageNumber);

            // ensure changes were applied
            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.CreateTree("foo");
                Assert.Null(tree.Read(new string('0', keySize)));

                var readResult = tree.Read(new string('4', 1000));

                Assert.Equal(21, readResult.Reader.Length);
            }
        }
    }
}