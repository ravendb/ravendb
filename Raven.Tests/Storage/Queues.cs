//-----------------------------------------------------------------------
// <copyright file="Queues.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Linq;
using Xunit;

namespace Raven.Tests.Storage
{
	public class Queues : RavenTest
	{
		[Fact]
		public void CanEnqueueAndPeek()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator => mutator.Queue.EnqueueToQueue("ayende", new byte[] {1, 2}));

				tx.Batch(
					mutator => Assert.Equal(new byte[] {1, 2}, mutator.Queue.PeekFromQueue("ayende").First().Item1));
			}
		}

		[Fact]
		public void PoisonMessagesWillBeDeleted()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator => mutator.Queue.EnqueueToQueue("ayende", new byte[] {1, 2}));

				tx.Batch(mutator =>
				{
					for (int i = 0; i < 6; i++)
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
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator => mutator.Queue.EnqueueToQueue("ayende", new byte[] { 1, 2 }));

				tx.Batch(mutator => 
				{
					mutator.Queue.DeleteFromQueue("ayende", mutator.Queue.PeekFromQueue("ayende").First().Item2);
					Assert.Equal(null, mutator.Queue.PeekFromQueue("ayende").FirstOrDefault());
				});
			}
		}
	}
}