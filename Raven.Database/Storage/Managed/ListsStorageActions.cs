using System;
using System.Collections.Generic;
using System.IO;
using Raven.Database.Impl;
using Raven.Database.Storage;
using Raven.Database.Util;
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

		public void Set(string name, string key, RavenJObject data)
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

		public IEnumerable<ListItem> Read(string name, Guid start, int take)
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
				return new ListItem
				{
					Data = readResult.Data().ToJObject(),
					Etag = new Guid(readResult.Key.Value<byte[]>("etag")),
					Key = readResult.Key.Value<string>("key")
				};
			})
			.Take(take);
		}

		public void RemoveAllBefore(string name, Guid etag)
		{
			var comparable = new ComparableByteArray(etag);
			var results = storage.Lists["ByNameAndEtag"].SkipAfter(new RavenJObject
			{
				{"name", name},
				{"etag", Guid.Empty.ToByteArray()}
			})
				.TakeWhile(x => String.Equals(x.Value<string>("name"), name, StringComparison.InvariantCultureIgnoreCase) &&
				                comparable.CompareTo(x.Value<byte[]>("etag")) >= 0);

			foreach (var result in results)
			{
				storage.Lists.Remove(result);
			}
		}

		public ListItem Read(string name, string key)
		{
			var readResult = storage.Lists.Read(new RavenJObject
			{
				{"name", name}, {"key", key}
			});


			if (readResult == null)
				return null;

			return new ListItem
			{
				Data = readResult.Data().ToJObject(),
				Key = readResult.Key.Value<string>("key"),
				Etag = new Guid(readResult.Key.Value<byte[]>("etag"))
			};
		}
	}
}