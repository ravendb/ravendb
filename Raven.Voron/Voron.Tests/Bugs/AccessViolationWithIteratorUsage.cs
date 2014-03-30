// -----------------------------------------------------------------------
//  <copyright file="AccessViolationWithIteratorUsage.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using Xunit;

namespace Voron.Tests.Bugs
{
	public class AccessViolationWithIteratorUsage : StorageTest
	{
		protected override void Configure(StorageEnvironmentOptions options)
		{
			options.ManualFlushing = true;
		}

		[Fact]
		public void ShouldNotThrow()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var tree = Env.CreateTree(tx, "test");

				tree.Add(tx, "items/1", new MemoryStream());
				tree.Add(tx, "items/2", new MemoryStream());

				tx.Commit();
			}

			using (var snapshot = Env.CreateSnapshot())
			using (var iterator = snapshot.Iterate("test"))
			{
				using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
				{
					for (int i = 0; i < 10; i++)
					{
						Env.State.GetTree(tx, "test").Add(tx, "items/" + i, new MemoryStream(new byte[2048]));
					}

					tx.Commit();
				}

				Assert.True(iterator.Seek(Slice.BeforeAllKeys));

				using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
				{
					for (int i = 10; i < 40; i++)
					{
						Env.State.GetTree(tx, "test").Add(tx, "items/" + i, new MemoryStream(new byte[2048]));
					}

					tx.Commit();
				}

				iterator.MoveNext();
			}
		}
	}
}