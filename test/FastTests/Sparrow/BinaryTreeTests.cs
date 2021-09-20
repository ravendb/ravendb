using System;
using Sparrow.Server.Binary;
using Sparrow.Server.Collections.Persistent;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sparrow
{
    public class BinaryTreeTests : NoDisposalNeeded
    {
        public BinaryTreeTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void EmptyTree()
        {
            Span<byte> storage = stackalloc byte[32];

            var tree = BinaryTree<int>.Create(storage);
            Assert.Equal(2, tree.MaxNodes);

            storage = stackalloc byte[64];
            tree = new BinaryTree<int>(storage);
            Assert.Equal(6, tree.MaxNodes);
        }

        [Fact]
        public void SingleBitTree()
        {
            Span<byte> storage = stackalloc byte[32];

            var tree = BinaryTree<int>.Create(storage);

            Span<byte> value = stackalloc byte[1];
            value[0] = 0x80;

            var reader = new BitReader(value, 1);
            tree.Add(ref reader, 999);

            reader = new BitReader(value, 1);
            Assert.True(tree.Find(ref reader, out var result));

            Assert.Equal(999, result);
        }

        [Fact]
        public void Single2BitTree()
        {
            Span<byte> storage = stackalloc byte[64];

            var tree = BinaryTree<int>.Create(storage);

            Span<byte> value = stackalloc byte[1];

            byte[] values = new[] { (byte)0x80, (byte)0x00 };
            for (int i = 0; i < values.Length; i++)
            {
                value[0] = values[i];

                var reader = new BitReader(value, 1);
                tree.Add(ref reader, 999 + i);

            }

            for (int i = 0; i < values.Length; i++)
            {
                value[0] = values[i];

                var reader = new BitReader(value, 1);
                Assert.True(tree.Find(ref reader, out var result));
                Assert.Equal(999 + i, result);
            }
        }

        [Fact]
        public void MultipleBitTree()
        {
            Span<byte> storage = stackalloc byte[256];

            var tree = BinaryTree<int>.Create(storage);

            Span<byte> value = stackalloc byte[1];

            byte[] values = new[] { (byte)0x80, (byte)0xCC, (byte)0x00 };
            for (int i = 0; i < values.Length; i++)
            {
                value[0] = values[i];

                var reader = new BitReader(value, 3);
                tree.Add(ref reader, 999 + i);
            }

            for (int i = 0; i < values.Length; i++)
            {
                value[0] = values[i];

                var reader = new BitReader(value, 3);
                Assert.True(tree.Find(ref reader, out var result));
                Assert.Equal((999 + i), result);
            }
        }

        [Fact]
        public void CommonPrefixBitTree()
        {
            Span<byte> storage = stackalloc byte[128];

            var tree = BinaryTree<int>.Create(storage);

            Span<byte> value = stackalloc byte[1];

            byte[] values = { 0x6A, 0xB4 };
            for (int i = 0; i < values.Length; i++)
            {
                value[0] = values[i];

                for (int j = 0; j < 8 - 3; j++)
                {
                    var reader = new BitReader(value, 3 + j);
                    tree.Add(ref reader, 999 + j);
                }
            }

            for (int i = 0; i < values.Length; i++)
            {
                value[0] = values[i];

                var reader = new BitReader(value);
                Assert.True(tree.FindCommonPrefix(ref reader, out var result));
                Assert.Equal(999, result);
            }
        }
    }
}
