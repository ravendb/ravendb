// -----------------------------------------------------------------------
//  <copyright file="Lists.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Json.Linq;

namespace Raven.Database.Storage.Esent.StorageActions
{
	public partial class DocumentStorageActions : IListsStorageActions
	{
		public void Set(string name, string key, RavenJObject data, UuidType uuidType)
		{
			Api.JetSetCurrentIndex(session, Lists, "by_name_and_key");
			Api.MakeKey(session, Lists, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, Lists, key, Encoding.Unicode, MakeKeyGrbit.None);

			var exists = Api.TrySeek(session, Lists, SeekGrbit.SeekEQ);


			using (var update = new Update(session, Lists, exists ? JET_prep.Replace : JET_prep.Insert))
			{
				Api.SetColumn(session, Lists, tableColumnsCache.ListsColumns["name"], name, Encoding.Unicode);
				Api.SetColumn(session, Lists, tableColumnsCache.ListsColumns["key"], key, Encoding.Unicode);
				Api.SetColumn(session, Lists, tableColumnsCache.ListsColumns["etag"], uuidGenerator.CreateSequentialUuid(uuidType).TransformToValueForEsentSorting());
				Api.SetColumn(session, Lists, tableColumnsCache.ListsColumns["created_at"], SystemTime.UtcNow);

				using (var columnStream = new ColumnStream(session, Lists, tableColumnsCache.ListsColumns["data"]))
				{
					if (exists)
						columnStream.SetLength(0);
					using (Stream stream = new BufferedStream(columnStream))
					{
						data.WriteTo(stream);
						stream.Flush();
					}
				}
				update.Save();
			}
		}

		public void Remove(string name, string key)
		{
			Api.JetSetCurrentIndex(session, Lists, "by_name_and_key");
			Api.MakeKey(session, Lists, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, Lists, key, Encoding.Unicode, MakeKeyGrbit.None);

			if (Api.TrySeek(session, Lists, SeekGrbit.SeekEQ))
				Api.JetDelete(session, Lists);
		}

		public void RemoveAllBefore(string name, Etag etag)
		{
			Api.JetSetCurrentIndex(session, Lists, "by_name_and_etag");
			Api.MakeKey(session, Lists, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, Lists, etag.TransformToValueForEsentSorting(), MakeKeyGrbit.None);
			if (Api.TrySeek(session, Lists, SeekGrbit.SeekLE) == false)
				return;
			do
			{
				var nameFromDb = Api.RetrieveColumnAsString(session, Lists, tableColumnsCache.ListsColumns["name"], Encoding.Unicode);
				if (string.Equals(name, nameFromDb, StringComparison.OrdinalIgnoreCase) == false)
					break;

				Api.JetDelete(session, Lists);

			} while (Api.TryMovePrevious(session, Lists));

		}

		public void RemoveAllOlderThan(string name, DateTime dateTime)
		{
			Api.JetSetCurrentIndex(session, Lists, "by_name_and_created_at");
			Api.MakeKey(session, Lists, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, Lists, dateTime, MakeKeyGrbit.None);
			if (Api.TrySeek(session, Lists, SeekGrbit.SeekLE) == false)
				return;
			do
			{
				var nameFromDb = Api.RetrieveColumnAsString(session, Lists, tableColumnsCache.ListsColumns["name"], Encoding.Unicode);
				if (string.Equals(name, nameFromDb, StringComparison.OrdinalIgnoreCase) == false)
					break;

				Api.JetDelete(session, Lists);

				MaybePulseTransaction();

			} while (Api.TryMovePrevious(session, Lists));
		}

		public IEnumerable<ListItem> Read(string name, Etag start, Etag end, int take)
		{
			Api.JetSetCurrentIndex(session, Lists, "by_name_and_etag");
			Api.MakeKey(session, Lists, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, Lists, start.TransformToValueForEsentSorting(), MakeKeyGrbit.None);
			if (Api.TrySeek(session, Lists, SeekGrbit.SeekGT) == false)
				yield break;
		
			int count = 0;
			do
			{
				var nameFromDb = Api.RetrieveColumnAsString(session, Lists, tableColumnsCache.ListsColumns["name"], Encoding.Unicode);
				if (string.Equals(name, nameFromDb, StringComparison.InvariantCultureIgnoreCase) == false)
					yield break;


				var etag = Etag.Parse(Api.RetrieveColumn(session, Lists, tableColumnsCache.ListsColumns["etag"]));
				if (end != null && end.CompareTo(etag) <= 0)
					yield break;

				count++;
				
				using (Stream stream = new BufferedStream(new ColumnStream(session, Lists, tableColumnsCache.ListsColumns["data"])))
				{
					yield return new ListItem
					{
						Etag = etag,
						Data = stream.ToJObject(),
						Key = Api.RetrieveColumnAsString(session, Lists, tableColumnsCache.ListsColumns["key"], Encoding.Unicode),
						CreatedAt =  Api.RetrieveColumnAsDateTime(session, Lists, tableColumnsCache.ListsColumns["created_at"]).Value
					};
				}
			} while (Api.TryMoveNext(session, Lists) && count < take);

		}

		public IEnumerable<ListItem> Read(string name, int start, int take)
		{
			Api.JetSetCurrentIndex(session, Lists, "by_name_and_etag");
			Api.MakeKey(session, Lists, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Lists, SeekGrbit.SeekGT) == false)
				yield break;

			int skipped = 0;
			while (skipped < start)
			{
				if (!Api.TryMoveNext(session, Lists))
					yield break;
				skipped++;
			}

			int count = 0;
			do
			{
				var nameFromDb = Api.RetrieveColumnAsString(session, Lists, tableColumnsCache.ListsColumns["name"], Encoding.Unicode);
				if (string.Equals(name, nameFromDb, StringComparison.InvariantCultureIgnoreCase) == false)
					yield break;


				var etag = Etag.Parse(Api.RetrieveColumn(session, Lists, tableColumnsCache.ListsColumns["etag"]));

				count++;

				using (Stream stream = new BufferedStream(new ColumnStream(session, Lists, tableColumnsCache.ListsColumns["data"])))
				{
					yield return new ListItem
					{
						Etag = etag,
						Data = stream.ToJObject(),
						Key = Api.RetrieveColumnAsString(session, Lists, tableColumnsCache.ListsColumns["key"], Encoding.Unicode),
						CreatedAt = Api.RetrieveColumnAsDateTime(session, Lists, tableColumnsCache.ListsColumns["created_at"]).Value
					};
				}
			} while (Api.TryMoveNext(session, Lists) && count < take);
		}

		public ListItem Read(string name, string key)
		{
			Api.JetSetCurrentIndex(session, Lists, "by_name_and_key");
			Api.MakeKey(session, Lists, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, Lists, key, Encoding.Unicode, MakeKeyGrbit.None);

			if (Api.TrySeek(session, Lists, SeekGrbit.SeekEQ) == false)
				return null;

			using (Stream stream = new BufferedStream(new ColumnStream(session, Lists, tableColumnsCache.ListsColumns["data"])))
			{
				return new ListItem
				{
					Data = stream.ToJObject(),
					Key = Api.RetrieveColumnAsString(session, Lists, tableColumnsCache.ListsColumns["key"], Encoding.Unicode),
					Etag = Etag.Parse(Api.RetrieveColumn(session, Lists, tableColumnsCache.ListsColumns["etag"])),
					CreatedAt = Api.RetrieveColumnAsDateTime(session, Lists, tableColumnsCache.ListsColumns["created_at"]).Value
				};
			}
		}

        public ListItem ReadLast(string name)
        {
            Api.JetSetCurrentIndex(session, Lists, "by_name_and_etag");
            Api.MakeKey(session, Lists, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
            Api.MakeKey(session, Lists, Etag.InvalidEtag.TransformToValueForEsentSorting(), MakeKeyGrbit.None);

            if (Api.TrySeek(session, Lists, SeekGrbit.SeekLE) == false)
                return null;

            var nameFromDb = Api.RetrieveColumnAsString(session, Lists, tableColumnsCache.ListsColumns["name"], Encoding.Unicode);
            if (string.Equals(name, nameFromDb, StringComparison.InvariantCultureIgnoreCase) == false) 
                return null;

            using (Stream stream = new BufferedStream(new ColumnStream(session, Lists, tableColumnsCache.ListsColumns["data"])))
            {
                return new ListItem
                {
                    Data = stream.ToJObject(),
                    Key = Api.RetrieveColumnAsString(session, Lists, tableColumnsCache.ListsColumns["key"], Encoding.Unicode),
                    Etag = Etag.Parse(Api.RetrieveColumn(session, Lists, tableColumnsCache.ListsColumns["etag"])),
					CreatedAt = Api.RetrieveColumnAsDateTime(session, Lists, tableColumnsCache.ListsColumns["created_at"]).Value
                };
            }
        }
	}
}
