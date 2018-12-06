// -----------------------------------------------------------------------
//  <copyright file="Lists.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
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
        public Etag Set(string name, string key, RavenJObject data, UuidType uuidType)
        {
            Api.JetSetCurrentIndex(session, Lists, "by_name_and_key");
            Api.MakeKey(session, Lists, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
            Api.MakeKey(session, Lists, key, Encoding.Unicode, MakeKeyGrbit.None);

            var exists = Api.TrySeek(session, Lists, SeekGrbit.SeekEQ);


            using (var update = new Update(session, Lists, exists ? JET_prep.Replace : JET_prep.Insert))
            {
                Api.SetColumn(session, Lists, tableColumnsCache.ListsColumns["name"], name, Encoding.Unicode);
                Api.SetColumn(session, Lists, tableColumnsCache.ListsColumns["key"], key, Encoding.Unicode);
                Etag etag = uuidGenerator.CreateSequentialUuid(uuidType);
                Api.SetColumn(session, Lists, tableColumnsCache.ListsColumns["etag"], etag.TransformToValueForEsentSorting());
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

                return etag;
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

        public void RemoveAllBefore(string name, Etag etag, TimeSpan? timeout = null)
        {
            Api.JetSetCurrentIndex(session, Lists, "by_name_and_etag");
            Api.MakeKey(session, Lists, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
            Api.MakeKey(session, Lists, etag.TransformToValueForEsentSorting(), MakeKeyGrbit.None);
            if (Api.TrySeek(session, Lists, SeekGrbit.SeekLE) == false)
                return;

            Stopwatch duration = null;
            if (timeout != null)
                duration = Stopwatch.StartNew();
            do
            {
                var nameFromDb = Api.RetrieveColumnAsString(session, Lists, tableColumnsCache.ListsColumns["name"], Encoding.Unicode);
                if (string.Equals(name, nameFromDb, StringComparison.OrdinalIgnoreCase) == false)
                    break;

                if (timeout != null && duration.Elapsed > timeout.Value)
                    break;

                Api.JetDelete(session, Lists);

                MaybePulseTransaction();

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

        public void Touch(string name, string key, UuidType uuidType, out Etag preTouchEtag, out Etag afterTouchEtag)
        {
            Api.JetSetCurrentIndex(session, Lists, "by_name_and_key");
            Api.MakeKey(session, Lists, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
            Api.MakeKey(session, Lists, key, Encoding.Unicode, MakeKeyGrbit.None);

            if (Api.TrySeek(session, Lists, SeekGrbit.SeekEQ) == false)
            {
                afterTouchEtag = null;
                preTouchEtag = null;
                return;
            }

            preTouchEtag = Etag.Parse(Api.RetrieveColumn(session, Lists, tableColumnsCache.ListsColumns["etag"]));

            using (var update = new Update(session, Lists, JET_prep.Replace))
            {
                afterTouchEtag = uuidGenerator.CreateSequentialUuid(uuidType);
                Api.SetColumn(session, Lists, tableColumnsCache.ListsColumns["etag"], afterTouchEtag.TransformToValueForEsentSorting());
                update.Save();
            }
        }

        public List<ListsInfo> GetListsStatsVerySlowly()
        {
            Api.JetSetCurrentIndex(session, Lists, "by_name_and_key");
            Api.MoveBeforeFirst(Session, Lists);
            string currentName = null;
            List<ListsInfo> res = new List<ListsInfo>();
            ListsInfo currentListsInfo = null;
            while (Api.TryMoveNext(Session, Lists))
            {
                //Since i iterate on an index that starts with name i now that a specific list item comes sequentially
                var name = Api.RetrieveColumnAsString(Session, Lists, tableColumnsCache.ListsColumns["name"],
                                                     Encoding.Unicode);
                if (currentName != name)
                {
                    if (currentListsInfo != null)
                    {
                        res.Add(currentListsInfo);
                        currentListsInfo.AverageListItemSizeOnDiskInBytes = currentListsInfo.SizeOnDiskInBytes / currentListsInfo.Count;
                    }
                    currentListsInfo = new ListsInfo { Name = name };
                }
                currentName = name;
                var sizeOnDisk = Api.RetrieveColumnSize(session, Lists, tableColumnsCache.ListsColumns["data"]);
                currentListsInfo.Count++;
                if (sizeOnDisk.HasValue)
                {
                    currentListsInfo.SizeOnDiskInBytes += sizeOnDisk.Value;
                    if (sizeOnDisk.Value > currentListsInfo.MaxListItemSizeOnDiskInBytes)
                        currentListsInfo.MaxListItemSizeOnDiskInBytes = sizeOnDisk.Value;
                    if (currentListsInfo.MinListItemSizeOnDiskInBytes == 0 || sizeOnDisk.Value < currentListsInfo.MinListItemSizeOnDiskInBytes)
                        currentListsInfo.MinListItemSizeOnDiskInBytes = sizeOnDisk.Value;
                }
            }
            res.Add(currentListsInfo);
            currentListsInfo.AverageListItemSizeOnDiskInBytes = currentListsInfo.SizeOnDiskInBytes / currentListsInfo.Count;
            res.Sort((a, b) => b.SizeOnDiskInBytes.CompareTo(a.SizeOnDiskInBytes));
            return res;
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
