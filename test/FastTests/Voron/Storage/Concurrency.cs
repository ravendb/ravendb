using System;

namespace Voron.Tests.Storage
{
    using System.IO;

    using Voron.Exceptions;
    using Voron.Impl;

    using Xunit;

    public class Concurrency : StorageTest
    {
        [Fact]
        public void MissingEntriesShouldReturn0Version()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");

                Assert.Equal(0, tree.ReadVersion("key/1"));
            }
        }

        [Fact]
        public void SimpleVersion()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tree.Add("key/1", StreamFor("123"));
                Assert.Equal(1, tree.ReadVersion("key/1"));
                tree.Add("key/1", StreamFor("123"));
                Assert.Equal(2, tree.ReadVersion("key/1"));

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.CreateTree("foo");
                Assert.Equal(2, tree.ReadVersion("key/1"));
            }
        }

        [Fact]
        public void VersionOverflow()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (uint i = 1; i <= ushort.MaxValue + 1; i++)
                {
                    tree.Add("key/1", StreamFor("123"));

                    var expected = i;
                    if (expected > ushort.MaxValue)
                        expected = 1;

                    Assert.Equal(expected, tree.ReadVersion("key/1"));
                }

                tx.Commit();
            }
        }

        [Fact]
        public void NoCommit()
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("foo");
                tx.Commit();
            }
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tree.Add("key/1", StreamFor("123"));
                Assert.Equal(1, tree.ReadVersion("key/1"));
                tree.Add("key/1", StreamFor("123"));
                Assert.Equal(2, tree.ReadVersion("key/1"));
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.CreateTree("foo");
                Assert.Equal(0, tree.ReadVersion("key/1"));
            }
        }

        [Fact]
        public void Delete()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tree.Add("key/1", StreamFor("123"));
                Assert.Equal(1, tree.ReadVersion("key/1"));

                tree.Delete("key/1");
                Assert.Equal(0, tree.ReadVersion("key/1"));
            }
        }

        [Fact]
        public void Missing()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tree.Add("key/1", StreamFor("123"), 0);
                Assert.Equal(1, tree.ReadVersion("key/1"));

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                var e = Assert.Throws<ConcurrencyException>(() => tree.Add("key/1", StreamFor("321"), 0));
                Assert.Equal("Cannot add 'key/1' to 'foo' tree. Version mismatch. Expected: 0. Actual: 1.", e.Message);
            }
        }

        [Fact]
        public void ConcurrencyExceptionShouldBeThrownWhenVersionMismatch()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tree.Add("key/1", StreamFor("123"));
                Assert.Equal(1, tree.ReadVersion("key/1"));

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                var e = Assert.Throws<ConcurrencyException>(() => tree.Add("key/1", StreamFor("321"), 2));
                Assert.Equal("Cannot add 'key/1' to 'foo' tree. Version mismatch. Expected: 2. Actual: 1.", e.Message);
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                var e = Assert.Throws<ConcurrencyException>(() => tree.Delete("key/1", 2));
                Assert.Equal("Cannot delete 'key/1' to 'foo' tree. Version mismatch. Expected: 2. Actual: 1.", e.Message);
            }
        }

        [Fact]
        public void ConcurrencyExceptionShouldBeThrownWhenVersionMismatchMultiTree()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tree.MultiAdd("key/1", "123");
                Assert.Equal(1, tree.ReadVersion("key/1"));

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                var e = Assert.Throws<ConcurrencyException>(() => tree.MultiAdd("key/1", "321", version: 2));
                Assert.Equal("Cannot add value '321' to key 'key/1' to 'foo' tree. Version mismatch. Expected: 2. Actual: 0.", e.Message);
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                var e = Assert.Throws<ConcurrencyException>(() => tree.MultiDelete("key/1", "123", 2));
                Assert.Equal("Cannot delete value '123' to key 'key/1' to 'foo' tree. Version mismatch. Expected: 2. Actual: 1.", e.Message);
            }
        }

    }
}