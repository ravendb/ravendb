// -----------------------------------------------------------------------
//  <copyright file="SplittingPageWithPrefixes.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using Xunit;

namespace SlowTests.Voron
{
    public class LongKeys : StorageTest
    {
        [Fact]
        public void ShouldHaveEnoughSpaceDuringTruncate()
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree( "tree");

                tx.Commit();
            }

            var r = new Random(1);

            var keys = new List<string>();

            for (int i = 0; i < 1000; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.ReadTree("tree");

                    var key1 = new string('a', r.Next(800)) + i;
                    tree.Add(key1, new MemoryStream(new byte[128]));
                    var key2 = new string('b', r.Next(800)) + i;
                    tree.Add(key2, new MemoryStream(new byte[256]));
                    var key3 = new string('c', r.Next(800)) + i;
                    tree.Add(key3, new MemoryStream(new byte[128]));
                    var key4 = new string('d', r.Next(500)) + i;
                    tree.Add(key4, new MemoryStream(new byte[512]));

                    tx.Commit();

                    keys.AddRange(new[] { key1, key2, key3, key4 });
                }
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("tree");

                for (int i = 0; i < keys.Count; i++)
                {
                    var key = keys[i];

                    Assert.NotNull(tree.Read(key));
                }
            }

            foreach (var key in keys)
            {
                using (var tx = Env.WriteTransaction())
                {
                    tx.ReadTree("tree").Delete(key); // this makes Debug.Assert(parentPage.NumberOfEntries >= 2) fail

                    tx.Commit();
                }
            }
        }

        [Theory]
        [InlineData(1)]
        [InlineData(3)]
        [InlineData(103)]
        public void ShouldHaveEnoughSpaceWhenSplittingPageInHalf(int seed)
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree");

                tx.Commit();
            }

            var r = new Random(seed);
            var keys = new List<string>();

            for (int i = 0; i < 1000; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.ReadTree("tree");

                    var key1 = new string('a', r.Next(1987)) + i;
                    tree.Add(key1, new MemoryStream(new byte[128]));
                    var key2 = new string('b', r.Next(1000)) + i;
                    tree.Add(key2, new MemoryStream(new byte[256]));
                    var key3 = new string('c', r.Next(1987)) + i;
                    tree.Add(key3, new MemoryStream(new byte[128]));
                    var key4 = new string('d', r.Next(500)) + i;
                    tree.Add(key4, new MemoryStream(new byte[512]));

                    tx.Commit();

                    keys.AddRange(new[] { key1, key2, key3, key4 });
                }
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("tree");

                for (int i = 0; i < keys.Count; i++)
                {
                    var key = keys[i];

                    Assert.NotNull(tree.Read(key));
                }
            }

            foreach (var key in keys)
            {
                using (var tx = Env.WriteTransaction())
                {
                    tx.ReadTree("tree").Delete(key); // this makes Debug.Assert(parentPage.NumberOfEntries >= 2) fail

                    tx.Commit();
                }
            }
        }

        [Theory]
        [InlineData(0)]
        [InlineData(2)]
        [InlineData(4)]
        public void NoDebugAssertShouldThrownDuringRebalancing(int seed)
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree");

                tx.Commit();
            }

            var r = new Random(seed);

            var addedKeys = new List<string>(4000);

            for (int i = 0; i < 1000; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.ReadTree("tree");

                    var key1 = new string('a', r.Next(1993)) + i;
                    tree.Add(key1, new MemoryStream(new byte[128]));

                    var key2 = new string('b', r.Next(1000)) + i;
                    tree.Add(key2, new MemoryStream(new byte[256]));

                    var key3 = new string('c', r.Next(1993)) + i;
                    tree.Add(key3, new MemoryStream(new byte[128]));


                    var key4 = new string('d', r.Next(500)) + i;
                    tree.Add(key4, new MemoryStream(new byte[512]));

                    addedKeys.AddRange(new[] { key1, key2, key3, key4 });

                    tx.Commit();
                }
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("tree");

                for (int i = 0; i < addedKeys.Count; i++)
                {
                    var key = addedKeys[i];

                    Assert.NotNull(tree.Read(key));
                }
            }

            foreach (var key in addedKeys)
            {
                using (var tx = Env.WriteTransaction())
                {
                    tx.ReadTree("tree").Delete(key); // this makes Debug.Assert(parentPage.NumberOfEntries >= 2) fail

                    tx.Commit();
                }
            }
        }
    }
}