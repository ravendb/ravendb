using System.IO;
using System.Threading;
using Xunit;

namespace Voron.Tests.Bugs
{
	public class MemoryAccess : StorageTest
	{
		protected override void Configure(StorageEnvironmentOptions options)
		{
			base.Configure(options);
			options.ManualFlushing = true;
		}

		[Fact]
		public void ShouldNotThrowAccessViolation()
		{
			var trees = CreateTrees(Env, 1, "tree");

			for (int a = 0; a < 2; a++)
			{
				using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
				{
					foreach (var tree in trees)
					{
						tx.State.GetTree(tx, tree).Add(tx, string.Format("key/{0}/{1}/1", new string('0', 1000), a), new MemoryStream());
						tx.State.GetTree(tx, tree).Add(tx, string.Format("key/{0}/{1}/2", new string('0', 1000), a), new MemoryStream());
					}

					tx.Commit();
				}
			}

			using (var txr = Env.NewTransaction(TransactionFlags.Read))
			{
				foreach (var tree in trees)
				{
					using (var iterator = txr.State.GetTree(txr, tree).Iterate(txr))
					{
						if (!iterator.Seek(Slice.BeforeAllKeys))
							continue;

						Env.FlushLogToDataFile();

						using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
						{
							txw.State.GetTree(txw, tree).Add(txw, string.Format("key/{0}/0/0", new string('0', 1000)), new MemoryStream());

							txw.Commit();
						}

						Thread.Sleep(1000);

						do
						{
							Assert.Contains("key/", iterator.CurrentKey.ToString());
						}
						while (iterator.MoveNext());
					}
				}
			}
		}
	}
}
