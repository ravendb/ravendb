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
using Raven.Abstractions.Util;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Raven.Abstractions.Extensions;

namespace Raven.Storage.Esent.StorageActions
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

			    MaybePulseTransaction();

            } while (Api.TryMovePrevious(session, Lists));

		}

	    public void RemoveAllOlderThan(string name, DateTime time)
	    {
	        var fromStart = Etag.Empty;
	        Api.JetSetCurrentIndex(session, Lists, "by_name_and_etag");
	        Api.MakeKey(session, Lists, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
	        Api.MakeKey(session, Lists, fromStart.TransformToValueForEsentSorting(), MakeKeyGrbit.None);
	        if (Api.TrySeek(session, Lists, SeekGrbit.SeekGE) == false)
	            return;
	        do
	        {
	            var nameFromDb = Api.RetrieveColumnAsString(session, Lists, tableColumnsCache.ListsColumns["name"], Encoding.Unicode);
	            if (string.Equals(name, nameFromDb, StringComparison.OrdinalIgnoreCase) == false)
	                break;
	            using (Stream stream = new BufferedStream(new ColumnStream(session, Lists, tableColumnsCache.ListsColumns["data"])))
	            {
	                var data = stream.ToJObject();

	                if (data.ContainsKey(Constants.LastModified) == false)
	                {
	                    Api.JetDelete(session, Lists);
	                    MaybePulseTransaction(); //If we have alot of those in a raw
	                    continue;
	                }

	                var lastModified = data.Value<DateTime>(Constants.LastModified);

	                if (lastModified > time)
	                    break;
                }
                Api.JetDelete(session, Lists);

	            MaybePulseTransaction();

	        } while (Api.TryMoveNext(session, Lists));

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
						Key = Api.RetrieveColumnAsString(session, Lists, tableColumnsCache.ListsColumns["key"], Encoding.Unicode)
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
					Etag = Etag.Parse(Api.RetrieveColumn(session, Lists, tableColumnsCache.ListsColumns["etag"]))
				};
			}
		}
	}
}
