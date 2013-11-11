using System.IO;
using System.Text;
using System.Threading.Tasks;
using Voron.Impl;
using Xunit;

namespace Voron.Tests.Storage
{
	public class Snapshots : StorageTest
	{
		[Fact]
		public void SingleItemBatchTest()
		{
			var batch = new WriteBatch();
            batch.Add("key/1", new MemoryStream(Encoding.UTF8.GetBytes("123")), Constants.RootTreeName);

			Env.Writer.Write(batch);

			using (var snapshot = Env.CreateSnapshot())
			{
				using (var stream = snapshot.Read(null, "key/1").Stream)
				using (var reader = new StreamReader(stream))
				{
					var result = reader.ReadToEnd();
					Assert.Equal("123", result);
				}
			}
		}

		[Fact]
		public void SingleItemBatchTestLowLevel()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				tx.State.Root.Add(tx, "key/1", new MemoryStream(Encoding.UTF8.GetBytes("123")));

				tx.Commit();
			}


			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				using(var stream = tx.State.Root.Read(tx, "key/1").Stream)
				using (var reader = new StreamReader(stream))
				{
					var result = reader.ReadToEnd();
					Assert.Equal("123", result);
				}
				tx.Commit();
			}
		}
	}
}