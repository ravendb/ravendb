// -----------------------------------------------------------------------
//  <copyright file="ListsStorageActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Storage.Voron
{
	using System.Collections.Generic;
	using System.IO;

	using Raven.Abstractions.Data;
	using Raven.Abstractions.Extensions;
	using Raven.Database.Impl;
	using Raven.Database.Storage.Voron.Impl;
	using Raven.Json.Linq;

	using global::Voron;
	using global::Voron.Impl;

	public class ListsStorageActions : IListsStorageActions
	{
		private readonly TableStorage tableStorage;

		private readonly IUuidGenerator generator;

		private readonly SnapshotReader snapshot;

		private readonly WriteBatch writeBatch;

		public ListsStorageActions(TableStorage tableStorage, IUuidGenerator generator, SnapshotReader snapshot, WriteBatch writeBatch)
		{
			this.tableStorage = tableStorage;
			this.generator = generator;
			this.snapshot = snapshot;
			this.writeBatch = writeBatch;
		}

		public void Set(string name, string key, RavenJObject data, UuidType type)
		{
			var listsByName = tableStorage.Lists.GetIndex(Tables.Lists.Indices.ByName);
			var listsByNameAndKey = tableStorage.Lists.GetIndex(Tables.Lists.Indices.ByNameAndKey);

			var etag = generator.CreateSequentialUuid(type);

			tableStorage.Lists.Add(
				writeBatch,
				etag.ToString(),
				new RavenJObject
				{
					{ "name", name }, 
					{ "key", key }, 
					{ "etag", etag.ToByteArray() }, 
					{ "data", data }
				}, 0);

			listsByName.MultiAdd(writeBatch, name, etag.ToString());
			listsByNameAndKey.Add(writeBatch, name + "/" + key, etag.ToString(), 0);
		}

		public void Remove(string name, string key)
		{
			var listsByName = tableStorage.Lists.GetIndex(Tables.Lists.Indices.ByName);
			var listsByNameAndKey = tableStorage.Lists.GetIndex(Tables.Lists.Indices.ByNameAndKey);

			var nameAndKey = name + "/" + key;

			using (var read = listsByNameAndKey.Read(snapshot, nameAndKey))
			{
				if (read == null)
					return;

				using (var reader = new StreamReader(read.Stream))
				{
					var etag = reader.ReadToEnd();
					tableStorage.Lists.Delete(writeBatch, etag);
					listsByName.MultiDelete(writeBatch, name, etag);
					listsByNameAndKey.Delete(writeBatch, nameAndKey);
				}
			}
		}

		public IEnumerable<ListItem> Read(string name, Etag start, Etag end, int take)
		{
			var listsByName = tableStorage.Lists.GetIndex(Tables.Lists.Indices.ByName);

			using (var iterator = listsByName.MultiRead(snapshot, name))
			{
				if (!iterator.Seek(start.ToString()))
					yield break;

				int count = 0;

				do
				{
					if (count >= take)
						yield break;

					var etag = Etag.Parse(iterator.CurrentKey.ToString());
					if (start.CompareTo(etag) > 0)
						continue;

					if (end != null && end.CompareTo(etag) < 0)
						yield break;

					count++;
					yield return ReadInternal(etag);
				}
				while (iterator.MoveNext());
			}
		}

		public ListItem Read(string name, string key)
		{
			var listsByNameAndKey = tableStorage.Lists.GetIndex(Tables.Lists.Indices.ByNameAndKey);
			var nameAndKey = name + "/" + key;

			using (var read = listsByNameAndKey.Read(snapshot, nameAndKey))
			{
				if (read == null)
					return null;

				using (var reader = new StreamReader(read.Stream))
				{
					var etag = reader.ReadToEnd();
					return ReadInternal(etag);
				}
			}
		}

		public void RemoveAllBefore(string name, Etag etag)
		{
			var listsByName = tableStorage.Lists.GetIndex(Tables.Lists.Indices.ByName);
			var listsByNameAndKey = tableStorage.Lists.GetIndex(Tables.Lists.Indices.ByNameAndKey);

			using (var iterator = listsByName.MultiRead(snapshot, name))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return;

				do
				{
					var currentEtag = Etag.Parse(iterator.CurrentKey.ToString());

					if (currentEtag.CompareTo(etag) < 0)
					{
						using (var read = tableStorage.Lists.Read(snapshot, iterator.CurrentKey))
						{
							var value = read.Stream.ToJObject();
							var key = value.Value<string>("key");

							tableStorage.Lists.Delete(writeBatch, currentEtag.ToString());
							listsByName.MultiDelete(writeBatch, name, etag.ToString());
							listsByNameAndKey.Delete(writeBatch, name + "/" + key);
						}
					}
				}
				while (iterator.MoveNext());
			}
		}

		private ListItem ReadInternal(string id)
		{
			using (var read = tableStorage.Lists.Read(snapshot, id))
			{
				if (read == null)
					return null;

				var value = read.Stream.ToJObject();
				var etag = Etag.Parse(value.Value<byte[]>("etag"));
				var k = value.Value<string>("key");

				return new ListItem
				{
					Data = value.Value<RavenJObject>("data"),
					Etag = etag,
					Key = k
				};
			}
		}
	}
}