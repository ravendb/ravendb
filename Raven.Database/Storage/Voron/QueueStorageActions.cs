// -----------------------------------------------------------------------
//  <copyright file="QueueStorageActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Storage.Voron
{
	using System;
	using System.Collections.Generic;

	using Raven.Abstractions.Data;
	using Raven.Abstractions.Extensions;
	using Raven.Database.Impl;
	using Raven.Database.Storage.Voron.Impl;
	using Raven.Json.Linq;

	using global::Voron;
	using global::Voron.Impl;

	public class QueueStorageActions : IQueueStorageActions
	{
		private readonly TableStorage tableStorage;

		private readonly SnapshotReader snapshot;

		private readonly WriteBatch writeBatch;

		private readonly IUuidGenerator generator;

		public QueueStorageActions(TableStorage tableStorage, IUuidGenerator generator, SnapshotReader snapshot, WriteBatch writeBatch)
		{
			this.tableStorage = tableStorage;
			this.snapshot = snapshot;
			this.writeBatch = writeBatch;
			this.generator = generator;
		}

		public void EnqueueToQueue(string name, byte[] data)
		{
			var queuesByName = tableStorage.Queues.GetIndex(Tables.Queues.Indices.ByName);

			var id = generator.CreateSequentialUuid(UuidType.Queue);
			var key = name + "/" + id;

			tableStorage.Queues.Add(writeBatch, key, new RavenJObject
			{
				{"name", name},
				{"id", id.ToByteArray()},
				{"reads", 0},
				{"data", data}
			}, 0);

			queuesByName.MultiAdd(writeBatch, name, key);
		}

		public IEnumerable<Tuple<byte[], object>> PeekFromQueue(string name)
		{
			var queuesByName = tableStorage.Queues.GetIndex(Tables.Queues.Indices.ByName);

			using (var iterator = queuesByName.MultiRead(snapshot, name))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					yield break;

				do
				{
					var key = iterator.CurrentKey;
					using (var read = tableStorage.Queues.Read(snapshot, key))
					{
						if (read == null)
							yield break;

						var value = read.Stream.ToJObject();
						if (value.Value<int>("reads") > 5) // read too much, probably poison message, remove it
						{
							tableStorage.Queues.Delete(writeBatch, key);
							continue;
						}

						value["reads"] = value.Value<int>("reads") + 1;
						tableStorage.Queues.Add(writeBatch, key, value);

						yield return new Tuple<byte[], object>(value.Value<byte[]>("data"), value.Value<byte[]>("id"));
					}
				}
				while (iterator.MoveNext());
			}
		}

		public void DeleteFromQueue(string name, object id)
		{
			var queuesByName = tableStorage.Queues.GetIndex(Tables.Queues.Indices.ByName);

			var key = name + "/" + Etag.Parse((byte[])id);
			tableStorage.Queues.Delete(writeBatch, key);
			queuesByName.MultiDelete(writeBatch, name, key);
		}
	}
}