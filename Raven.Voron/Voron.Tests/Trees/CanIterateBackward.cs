using System.IO;
using Xunit;

namespace Voron.Tests.Trees
{
	public class CanIterateBackward : StorageTest
	{
		[Fact]
		public void SeekLastOnEmptyResultInFalse()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			using (var it = tx.State.Root.Iterate(tx))
			{
				Assert.False(it.Seek(Slice.AfterAllKeys));

				tx.Commit();
			}
		}

		[Fact]
		public void CanSeekLast()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.Add(tx, "a", new MemoryStream(0));
				tx.State.Root.Add(tx, "c", new MemoryStream(0));
				tx.State.Root.Add(tx, "b", new MemoryStream(0));

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			using (var it = tx.State.Root.Iterate(tx))
			{
				Assert.True(it.Seek(Slice.AfterAllKeys));
				Assert.Equal("c", it.CurrentKey.ToString());

				tx.Commit();
			}
		}

		[Fact]
		public void CanSeekBack()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.Add(tx, "a", new MemoryStream(0));
				tx.State.Root.Add(tx, "c", new MemoryStream(0));
				tx.State.Root.Add(tx, "b", new MemoryStream(0));

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			using (var it = tx.State.Root.Iterate(tx))
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