// -----------------------------------------------------------------------
//  <copyright file="QueueStorageActionsTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Common;

namespace Raven.Tests.Storage.Voron
{
	using System.Linq;
	using System.Text;

	using Xunit;
	using Xunit.Extensions;

	[Trait("VoronTest", "StorageActionsTests")]
	public class QueueStorageActionsTests : TransactionalStorageTestBase
	{
		[Theory]
		[PropertyData("Storages")]
		public void SimpleQueue(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				var value = Encoding.UTF8.GetBytes("123");

				storage.Batch(accessor => accessor.Queue.EnqueueToQueue("queue1", value));

				storage.Batch(accessor =>
				{
					var tuples = accessor.Queue.PeekFromQueue("queue1").ToList();

					Assert.Equal(1, tuples.Count);

					var tuple = tuples[0];
					Assert.True(value.SequenceEqual(tuple.Item1));
					Assert.True(tuple.Item2 != null);

					accessor.Queue.DeleteFromQueue("queue1", tuple.Item2);
				});

				storage.Batch(
					accessor =>
					{
						var tuples = accessor.Queue.PeekFromQueue("queue1").ToList();
						Assert.Equal(0, tuples.Count);
					});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void QueueWithMultipleValues(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				var value1 = Encoding.UTF8.GetBytes("123");
				var value2 = Encoding.UTF8.GetBytes("321");

				storage.Batch(accessor =>
				{
					accessor.Queue.EnqueueToQueue("queue1", value1);
					accessor.Queue.EnqueueToQueue("queue1", value2);
				});

				storage.Batch(accessor =>
				{
					var tuples = accessor.Queue.PeekFromQueue("queue1").ToList();

					Assert.Equal(2, tuples.Count);

					var tuple1 = tuples[0];
					Assert.True(value1.SequenceEqual(tuple1.Item1));
					Assert.True(tuple1.Item2 != null);

					var tuple2 = tuples[1];
					Assert.True(value2.SequenceEqual(tuple2.Item1));
					Assert.True(tuple2.Item2 != null);

					accessor.Queue.DeleteFromQueue("queue1", tuple1.Item2);
				});

				storage.Batch(
					accessor =>
					{
						var tuples = accessor.Queue.PeekFromQueue("queue1").ToList();
						Assert.Equal(1, tuples.Count);

						var tuple = tuples[0];
						Assert.True(value2.SequenceEqual(tuple.Item1));
						Assert.True(tuple.Item2 != null);

						accessor.Queue.DeleteFromQueue("queue1", tuple.Item2);
					});

				storage.Batch(
					accessor =>
					{
						var tuples = accessor.Queue.PeekFromQueue("queue1").ToList();
						Assert.Equal(0, tuples.Count);
					});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void MultipleQueues(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				var value1 = Encoding.UTF8.GetBytes("123");
				var value2 = Encoding.UTF8.GetBytes("321");

				storage.Batch(
					accessor =>
					{
						accessor.Queue.EnqueueToQueue("queue1", value1);
						accessor.Queue.EnqueueToQueue("queue2", value2);
					});

				storage.Batch(
					accessor =>
					{
						var tuples1 = accessor.Queue.PeekFromQueue("queue1").ToList();

						Assert.Equal(1, tuples1.Count);

						var tuple = tuples1[0];
						Assert.True(value1.SequenceEqual(tuple.Item1));
						Assert.True(tuple.Item2 != null);

						var tuples2 = accessor.Queue.PeekFromQueue("queue2").ToList();

						Assert.Equal(1, tuples2.Count);

						tuple = tuples2[0];
						Assert.True(value2.SequenceEqual(tuple.Item1));
						Assert.True(tuple.Item2 != null);
					});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void PeekingMoreThan5TimesFromQueueShouldRemoveItem(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				var value = Encoding.UTF8.GetBytes("123");

				storage.Batch(accessor => accessor.Queue.EnqueueToQueue("queue1", value));
				for (int i = 0; i < 5; i++)
				{
					storage.Batch(accessor => Assert.Equal(1, accessor.Queue.PeekFromQueue("queue1").Count()));
				}

				storage.Batch(accessor => Assert.Equal(1, accessor.Queue.PeekFromQueue("queue1").Count()));
				storage.Batch(accessor => Assert.Equal(0, accessor.Queue.PeekFromQueue("queue1").Count()));
			}
		}
	}
}