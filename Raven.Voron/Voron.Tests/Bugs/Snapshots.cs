using System.Linq;
using Voron.Debugging;

namespace Voron.Tests.Bugs
{
	using System;
	using System.Collections.Generic;
	using System.Diagnostics;
	using System.IO;
	using System.Text;
	using System.Threading.Tasks;

	using Voron.Impl;
	using Voron.Trees;

	using Xunit;

	public class Snapshots : StorageTest
	{
		public Snapshots()
			: base(StorageEnvironmentOptions.CreateMemoryOnly())
		{
			
		}

		[Fact]
		public void SnapshotIssue()
		{
			const int DocumentCount = 50000;
			
			var rand = new Random();
			var testBuffer = new byte[39];
			rand.NextBytes(testBuffer);


			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.CreateTree(tx, "tree1");
				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
			    var t1 = tx.Environment.State.GetTree(tx,"tree1");
				for (var i = 0; i < DocumentCount; i++)
				{
					t1.Add(tx, "docs/" + i, new MemoryStream(testBuffer));
				}

				tx.Commit();
			}

			using (var snapshot = Env.CreateSnapshot())
			{
                using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
				{
				    var t1 = tx.Environment.State.GetTree(tx,"tree1");
					for (var i = 0; i < DocumentCount; i++)
					{
						t1.Delete(tx, "docs/" + i);
					}

					tx.Commit();
				}

				for (var i = 0; i < DocumentCount; i++)
				{
					var result = snapshot.Read("tree1", "docs/" + i);
					Assert.NotNull(result);

					{
						int used;
						Assert.Equal(testBuffer, result.Reader.ReadBytes(result.Reader.Length, out used).Take(used).ToArray());
					}
				}
			}
		}

		[Fact]
		public void SnapshotIssue_ExplicitFlushing()
		{
			const int DocumentCount = 50000;

			var rand = new Random();
			var testBuffer = new byte[39];
			rand.NextBytes(testBuffer);

			_options.ManualFlushing = true;

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				Env.CreateTree(tx, "tree1");
				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var t1 = tx.Environment.State.GetTree(tx, "tree1");
				for (var i = 0; i < DocumentCount; i++)
				{
					t1.Add(tx, "docs/" + i, new MemoryStream(testBuffer));
				}

				tx.Commit();
			}

			Env.FlushLogToDataFile();

			using (var snapshot = Env.CreateSnapshot())
			{
				using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
				{
					var t1 = tx.Environment.State.GetTree(tx, "tree1");
					for (var i = 0; i < DocumentCount; i++)
					{
						t1.Delete(tx, "docs/" + i);
					}

					tx.Commit();
				}

				Env.FlushLogToDataFile();

				for (var i = 0; i < DocumentCount; i++)
				{
					var result = snapshot.Read("tree1", "docs/" + i);
					Assert.NotNull(result);

					{
						int used;
						Assert.Equal(testBuffer, result.Reader.ReadBytes(result.Reader.Length, out used).Take(used).ToArray());
					}
				}
			}
		}
	}
}