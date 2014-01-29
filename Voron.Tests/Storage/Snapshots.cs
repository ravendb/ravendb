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
			    var reader = snapshot.Read(null, "key/1").Reader;
			    Assert.Equal("123", reader.ToStringValue());
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
                var reader = tx.State.Root.Read(tx, "key/1").Reader;
			    Assert.Equal("123", reader.ToStringValue());
			    tx.Commit();
			}
		}
	}
}