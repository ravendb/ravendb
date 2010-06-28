using System.Linq;
using Raven.Storage.Managed;
using Xunit;

namespace Raven.Storage.Tests
{
	public class Queues : TxStorageTest
	{
		[Fact]
		public void CanEnqueueAndPeek()
		{
			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.Queue.EnqueueToQueue("ayende", new byte[] {1, 2}));

				tx.Write(
					mutator => Assert.Equal(new byte[] {1, 2}, mutator.Queue.PeekFromQueue("ayende").First().Item1));
			}
		}

		[Fact]
		public void PoisonMessagesWillBeDeleted()
		{
			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.Queue.EnqueueToQueue("ayende", new byte[] {1, 2}));

				tx.Write(mutator =>
				{
					for (int i = 0; i < 5; i++)
					{
						mutator.Queue.PeekFromQueue("ayende").First();
					}
					Assert.Equal(null, mutator.Queue.PeekFromQueue("ayende").FirstOrDefault());
				});
			}
		}

		[Fact]
		public void CanDeleteQueuedData()
		{
			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator => mutator.Queue.EnqueueToQueue("ayende", new byte[] { 1, 2 }));

				tx.Write(mutator => 
				{
					mutator.Queue.DeleteFromQueue("ayende", mutator.Queue.PeekFromQueue("ayende").First().Item2);
					Assert.Equal(null, mutator.Queue.PeekFromQueue("ayende").FirstOrDefault());
				});
			}
		}
	}
}