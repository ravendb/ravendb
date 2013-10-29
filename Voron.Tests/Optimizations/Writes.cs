// -----------------------------------------------------------------------
//  <copyright file="Foo.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using Xunit;

namespace Voron.Tests.Optimizations
{
	public class Writes : StorageTest
	{
		[Fact]
		public void SinglePageModificationDoNotCauseCopyingAllIntermediatePages()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.Add(tx, new string('9', 1300), new MemoryStream(new byte[3]));
				RenderAndShow(tx, tx.State.Root, 1);
				tx.State.Root.Add(tx, new string('1', 1300), new MemoryStream(new byte[3]));
				RenderAndShow(tx, tx.State.Root, 1);
				tx.State.Root.Add(tx, new string('4', 1000), new MemoryStream(new byte[2]));
				RenderAndShow(tx, tx.State.Root, 1);
				tx.State.Root.Add(tx, new string('5', 1300), new MemoryStream(new byte[2]));
				RenderAndShow(tx, tx.State.Root, 1);
				tx.State.Root.Add(tx, new string('8', 1300), new MemoryStream(new byte[3]));
				RenderAndShow(tx, tx.State.Root, 1);
				tx.State.Root.Add(tx, new string('2', 1300), new MemoryStream(new byte[2]));
				RenderAndShow(tx, tx.State.Root, 1);
				tx.State.Root.Add(tx, new string('6', 1300), new MemoryStream(new byte[2]));
				RenderAndShow(tx, tx.State.Root, 1);
				tx.State.Root.Add(tx, new string('0', 1300), new MemoryStream(new byte[4]));
				RenderAndShow(tx, tx.State.Root, 1);
				tx.State.Root.Add(tx, new string('3', 1000), new MemoryStream(new byte[1]));
				RenderAndShow(tx, tx.State.Root, 1);
				tx.State.Root.Add(tx, new string('7', 1300), new MemoryStream(new byte[1]));
				
				tx.Commit();
			}

			var afterAdds = Env.NextPageNumber;

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.Delete(tx, new string('0', 1300));

				tx.State.Root.Add(tx, new string('4', 1000), new MemoryStream(new byte[21]));

				tx.Commit();
			}

			Assert.Equal(afterAdds, Env.NextPageNumber);

			// ensure changes were applied
			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				Assert.Null(tx.State.Root.Read(tx, new string('0', 1300)));

				var readResult = tx.State.Root.Read(tx, new string('4', 1000));

				Assert.Equal(21, readResult.Stream.Length);
			}
		}
	}
}