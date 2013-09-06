// -----------------------------------------------------------------------
//  <copyright file="ListsStorageActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Storage.Voron.StorageActions
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

	public class ListsStorageActions : StorageActionsBase, IListsStorageActions
	{
		private readonly TableStorage tableStorage;

		private readonly IUuidGenerator generator;

		private readonly WriteBatch writeBatch;

		public ListsStorageActions(TableStorage tableStorage, IUuidGenerator generator, SnapshotReader snapshot, WriteBatch writeBatch)
			: base(snapshot)
		{
			this.tableStorage = tableStorage;
			this.generator = generator;
			this.writeBatch = writeBatch;
		}

		public void Set(string name, string key, RavenJObject data, UuidType type)
		{
			var listsByName = this.tableStorage.Lists.GetIndex(Tables.Lists.Indices.ByName);
			var listsByNameAndKey = this.tableStorage.Lists.GetIndex(Tables.Lists.Indices.ByNameAndKey);

			var etag = this.generator.CreateSequentialUuid(type);
			var etagAsString = etag.ToString();

			this.tableStorage.Lists.Add(
				this.writeBatch,
				etagAsString,
				new RavenJObject
				{
					{ "name", name }, 
					{ "key", key }, 
					{ "etag", etag.ToByteArray() }, 
					{ "data", data }
				});

			listsByName.MultiAdd(this.writeBatch, name, etagAsString);
			listsByNameAndKey.Add(this.writeBatch, this.CreateKey(name, key), etagAsString);
		}

		public void Remove(string name, string key)
		{
			var listsByName = this.tableStorage.Lists.GetIndex(Tables.Lists.Indices.ByName);
			var listsByNameAndKey = this.tableStorage.Lists.GetIndex(Tables.Lists.Indices.ByNameAndKey);

			var nameAndKey = this.CreateKey(name, key);

			using (var read = listsByNameAndKey.Read(this.Snapshot, nameAndKey))
			{
				if (read == null)
					return;

				using (var reader = new StreamReader(read.Stream))
				{
					var etag = reader.ReadToEnd();
					this.tableStorage.Lists.Delete(this.writeBatch, etag);
					listsByName.MultiDelete(this.writeBatch, name, etag);
					listsByNameAndKey.Delete(this.writeBatch, nameAndKey);
				}
			}
		}

		public IEnumerable<ListItem> Read(string name, Etag start, Etag end, int take)
		{
			var listsByName = this.tableStorage.Lists.GetIndex(Tables.Lists.Indices.ByName);

			using (var iterator = listsByName.MultiRead(this.Snapshot, name))
			{
				if (!iterator.Seek(start.ToString()) || !iterator.MoveNext())
					yield break;

				int count = 0;

				do
				{
					if (count >= take)
						yield break;

					var etag = Etag.Parse(iterator.CurrentKey.ToString());
					if (start.CompareTo(etag) > 0)
						continue;

					if (end != null && end.CompareTo(etag) <= 0)
						yield break;

					count++;
					yield return this.ReadInternal(etag);
				}
				while (iterator.MoveNext());
			}
		}

		public ListItem Read(string name, string key)
		{
			var listsByNameAndKey = this.tableStorage.Lists.GetIndex(Tables.Lists.Indices.ByNameAndKey);
			var nameAndKey = this.CreateKey(name, key);

			using (var read = listsByNameAndKey.Read(this.Snapshot, nameAndKey))
			{
				if (read == null)
					return null;

				using (var reader = new StreamReader(read.Stream))
				{
					var etag = reader.ReadToEnd();
					return this.ReadInternal(etag);
				}
			}
		}

		public void RemoveAllBefore(string name, Etag etag)
		{
			var listsByName = this.tableStorage.Lists.GetIndex(Tables.Lists.Indices.ByName);
			var listsByNameAndKey = this.tableStorage.Lists.GetIndex(Tables.Lists.Indices.ByNameAndKey);

			using (var iterator = listsByName.MultiRead(this.Snapshot, name))
			{
				if (!iterator.Seek(Slice.BeforeAllKeys))
					return;

				do
				{
					var currentEtag = Etag.Parse(iterator.CurrentKey.ToString());

					if (currentEtag.CompareTo(etag) < 0)
					{
						using (var read = this.tableStorage.Lists.Read(this.Snapshot, iterator.CurrentKey))
						{
							var value = read.Stream.ToJObject();
							var key = value.Value<string>("key");

							this.tableStorage.Lists.Delete(this.writeBatch, currentEtag.ToString());
							listsByName.MultiDelete(this.writeBatch, name, etag.ToString());
							listsByNameAndKey.Delete(this.writeBatch, this.CreateKey(name, key));
						}
					}
				}
				while (iterator.MoveNext());
			}
		}

		private ListItem ReadInternal(string id)
		{
			using (var read = this.tableStorage.Lists.Read(this.Snapshot, id))
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