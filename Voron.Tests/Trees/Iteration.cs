using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Voron.Impl;
using Xunit;

namespace Voron.Tests.Trees
{
	public unsafe class Iteration : StorageTest
	{
		[Fact]
		public void EmptyIterator()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var iterator = tx.State.Root.Iterate(tx);
				Assert.False(iterator.Seek(Slice.BeforeAllKeys));
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var iterator = tx.State.Root.Iterate(tx);
				Assert.False(iterator.Seek(Slice.AfterAllKeys));
			}
		}

		[Fact]
		public void CanIterateInOrder()
		{
			var random = new Random();
			var buffer = new byte[512];
			random.NextBytes(buffer);

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (int i = 0; i < 25; i++)
				{
					tx.State.Root.Add(tx, i.ToString("0000"), new MemoryStream(buffer));
				}

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var iterator = tx.State.Root.Iterate(tx);
				Assert.True(iterator.Seek(Slice.BeforeAllKeys));

				var slice = new Slice(SliceOptions.Key);
				for (int i = 0; i < 24; i++)
				{
					slice.Set(iterator.Current);
					Assert.Equal(i.ToString("0000"), slice);

					Assert.True(iterator.MoveNext());
				}

				slice.Set(iterator.Current);

				Assert.Equal(24.ToString("0000"), slice);
				Assert.False(iterator.MoveNext());
			}
		}

	}
}