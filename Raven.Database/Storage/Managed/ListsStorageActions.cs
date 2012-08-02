using System;
using System.Collections.Generic;
using System.IO;
using Raven.Database.Impl;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Raven.Storage.Managed.Impl;
using Raven.Abstractions.Extensions;
using System.Linq;

namespace Raven.Storage.Managed
{
	public class ListsStorageActions : IListsStorageActions
	{
		private readonly TableStorage storage;
		private readonly IUuidGenerator generator;

		public ListsStorageActions(TableStorage storage, IUuidGenerator generator)
		{
			this.storage = storage;
			this.generator = generator;
		}

		public void Add(string name, string key, RavenJObject data)
		{
			var memoryStream = new MemoryStream();
			data.WriteTo(memoryStream);
			storage.Lists.Put(new RavenJObject
			{
				{"name", name},
				{"key", key},
				{"etag", generator.CreateSequentialUuid().ToByteArray()}
			}, memoryStream.ToArray());
		}

		public void Remove(string name, string key)
		{
			var readResult = storage.Lists.Read(new RavenJObject
			{
				{"name", name}, {"key", key}
			});

			if (readResult == null)
				return;
			storage.Lists.Remove(readResult.Key);
		}

		public IEnumerable<Tuple<Guid, RavenJObject>> Read(string name, Guid start, int take)
		{
			return storage.Lists["ByNameAndEtag"].SkipAfter(new RavenJObject
			{
				{ "name", name },
				{ "etag", start.ToByteArray() }
			})
			.TakeWhile(x=> StringComparer.InvariantCultureIgnoreCase.Equals(x.Value<string>("name"), name))
			.Select(result =>
			{
				var readResult = storage.Lists.Read(result);
				return Tuple.Create(
					new Guid(readResult.Key.Value<byte[]>("etag")),
					readResult.Data().ToJObject());
			})
			.Take(take);
		}

		public RavenJObject Read(string name, string key)
		{
			var readResult = storage.Lists.Read(new RavenJObject
			{
				{"name", name}, {"key", key}
			});


			if (readResult == null)
				return null;

			return readResult.Data().ToJObject();
		}
	}
}