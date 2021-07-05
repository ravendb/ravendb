using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Voron;
using Voron.Data.Containers;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron
{
    public class ContainerTests : StorageTest
    {
        public ContainerTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanStoreData()
        {
            using var wtx = Env.WriteTransaction();

            var containerId = Container.Create(wtx.LowLevelTransaction);

            Span<byte> expected = Encoding.UTF8.GetBytes("Stav");

            var id = Container.Allocate(wtx.LowLevelTransaction, containerId, expected.Length, out var space);
            expected.CopyTo(space);

            var actual = Container.Get(wtx.LowLevelTransaction, id);
            
            Assert.Equal(expected.ToArray(), actual.ToSpan().ToArray());
        }

        
        [Fact]
        public void CanScanValues()
        {
            using var wtx = Env.WriteTransaction();

            var containerId = Container.Create(wtx.LowLevelTransaction);

            var expected = new List<long>();
            for (int i = 0; i < 1024; i++)
            {
                var id = Container.Allocate(wtx.LowLevelTransaction, containerId, 8, out _);
                expected.Add(id);
            }

            for (int i = 0; i < 16; i++)
            {
                var id = Container.Allocate(wtx.LowLevelTransaction, containerId, 10_000, out _);
                expected.Add(id);
            }
            
            expected.Sort();

            var actual = Container.GetAllIds(wtx.LowLevelTransaction, containerId);
            actual.Sort();
            Assert.Equal(expected.Count, actual.Count);
            Assert.Equal(expected, actual);
        }


        [Fact]
        public void CanStoreDeleteAndRecoverSpace()
        {
            using var wtx = Env.WriteTransaction();

            var containerId = Container.Create(wtx.LowLevelTransaction);

            Span<byte> expected = Encoding.UTF8.GetBytes("Stav");
            var ids = new List<long>();

            for (int i = 0; i < 16; i++)
            {
                var id = Container.Allocate(wtx.LowLevelTransaction, containerId, 500, out var s);
                expected.CopyTo(s);
                ids.Add(id);
            }

            for (int i = 0; i < 3; i++)
            {
                Container.Delete(wtx.LowLevelTransaction, containerId, ids[^1]);
                ids.RemoveAt(ids.Count - 1);
            }


            for (int i = 0; i < ids.Count; i++)
            {
                Container.Delete(wtx.LowLevelTransaction, containerId, ids[i]);
                ids.RemoveAt(i);
            }


            // should not throw here
            Container.Allocate(wtx.LowLevelTransaction, containerId, 750, out var space);

            foreach (var itemId in ids)
            {
                var data = Container.Get(wtx.LowLevelTransaction, itemId);
                Assert.True(data.ToSpan().Slice(0, 4).SequenceEqual(expected));
            }
        }

        [Fact]
        public void CanStoreMoreThanASinglePage()
        {
            using var wtx = Env.WriteTransaction();

            var containerId = Container.Create(wtx.LowLevelTransaction);

            Span<byte> expected = Encoding.UTF8.GetBytes("Stav");
            var ids = new List<long>();

            for (int i = 0; i < 16; i++)
            {
                var id = Container.Allocate(wtx.LowLevelTransaction, containerId, 500, out var s);
                expected.CopyTo(s);
                ids.Add(id);
            }

            // should not throw here
            var newId = Container.Allocate(wtx.LowLevelTransaction, containerId, 750, out var space);

            foreach (var itemId in ids)
            {
                var data = Container.Get(wtx.LowLevelTransaction, itemId);
                Assert.True(data.ToSpan().Slice(0, 4).SequenceEqual(expected));
            }
        }
    }
}
