using System;
using System.IO;
using Nevar.Debugging;
using Xunit;

namespace Nevar.Tests.Trees
{
	public class FreeSpace : StorageTest
	{
		[Fact]
		public void WillBeReused()
		{
			var random = new Random();
			var buffer = new byte[512];
			random.NextBytes(buffer);

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (int i = 0; i < 25; i++)
				{
					Env.Root.Add(tx, i.ToString("0000"), new MemoryStream(buffer));
				}

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (int i = 0; i < 25; i++)
				{
					Env.Root.Delete(tx, i.ToString("0000"));
				}

				tx.Commit();
			}

		    var old = Env.NextPageNumber;
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (int i = 0; i < 25; i++)
				{
					Env.Root.Add(tx, i.ToString("0000"), new MemoryStream(buffer));
				}

				tx.Commit();
			}

		    var stats = Env.Stats();

		    Assert.Equal(stats.FreePages + stats.FreePagesOverhead + stats.HeaderPages + stats.RootPages, 
                Env.NextPageNumber);

            Assert.Equal(old, Env.NextPageNumber);
		}
	}
}