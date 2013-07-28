using System;
using System.IO;
using Xunit;

namespace Nevar.Tests.Storage
{
	public class VeryBig : StorageTest
	{
		[Fact]
		public void CanGrowBeyondInitialSize()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.CreateTree(tx, "test");
				tx.Commit();
			}

			var buffer = new byte[1024 * 512];
			new Random().NextBytes(buffer);

			var tree = Env.GetTree("test");
			for (int i = 0; i < 20; i++)
			{
				using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
				{

					for (int j = 0; j < 12; j++)
					{
						tree.Add(tx, string.Format("{0:000}-{1:000}", j, i), new MemoryStream(buffer));
					}
					tx.Commit();
				}
			}
		}

		[Fact]
		public void CanGrowBeyondInitialSize_Root()
		{

			var buffer = new byte[1024 * 512];
			new Random().NextBytes(buffer);

			for (int i = 0; i < 20; i++)
			{
				using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
				{
					if (i == 17)
					{

					}
					for (int j = 0; j < 12; j++)
					{
						Env.Root.Add(tx, string.Format("{0:000}-{1:000}", j, i), new MemoryStream(buffer));
					}
					tx.Commit();
				}
			}
		}
		[Fact]
		public void CanGrowBeyondInitialSize_WithAnotherTree()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.CreateTree(tx, "test");
				tx.Commit();
			}
			var buffer = new byte[1024 * 512];
			new Random().NextBytes(buffer);

			for (int i = 0; i < 20; i++)
			{
				using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
				{

					for (int j = 0; j < 12; j++)
					{
						Env.Root.Add(tx, string.Format("{0:000}-{1:000}", j, i), new MemoryStream(buffer));
					}
					tx.Commit();
				}
			}
		}
	}
}