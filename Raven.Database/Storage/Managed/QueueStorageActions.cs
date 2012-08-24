//-----------------------------------------------------------------------
// <copyright file="QueueStorageActions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Database.Impl;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Raven.Storage.Managed.Impl;
using System.Linq;

namespace Raven.Storage.Managed
{
	public class QueueStorageActions : IQueueStorageActions
	{
		private readonly TableStorage storage;
		private readonly IUuidGenerator generator;

		public QueueStorageActions(TableStorage storage, IUuidGenerator generator)
		{
			this.storage = storage;
			this.generator = generator;
		}

		public void EnqueueToQueue(string name, byte[] data)
		{
			storage.Queues.Put(new RavenJObject
			{
				{"name", name},
				{"id", generator.CreateSequentialUuid().ToByteArray()},
				{"reads", 0}
			}, data);
		}

		public IEnumerable<Tuple<byte[], object>> PeekFromQueue(string name)
		{
			foreach (var queuedMsgKey in storage.Queues["ByName"].SkipTo(new RavenJObject
			{
				{"name", name}
			}).TakeWhile(x=> StringComparer.InvariantCultureIgnoreCase.Equals(x.Value<string>("name"), name)))
			{
				var readResult = storage.Queues.Read(queuedMsgKey);
				if(readResult == null)
					yield break;

				var ravenJToken = readResult.Key;
				if (ravenJToken.Value<int>("reads") > 5) //      // read too much, probably poison message, remove it
				{
					storage.Queues.Remove(ravenJToken);
					continue;
				}
				ravenJToken = ravenJToken.CloneToken();

				((RavenJObject)ravenJToken)["reads"] = ravenJToken.Value<int>("reads") + 1;
				storage.Queues.UpdateKey(ravenJToken);

				yield return new Tuple<byte[], object>(
					readResult.Data(),
					ravenJToken.Value<byte[]>("id")
					);
			}
		}

		public void DeleteFromQueue(string name, object id)
		{
			storage.Queues.Remove(new RavenJObject
				{
					{"name", name},
					{"id", (byte[])id}
				});
		}
	}
}