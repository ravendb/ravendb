// -----------------------------------------------------------------------
//  <copyright file="ListsStorageActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using Raven.Abstractions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util.Streams;

namespace Raven.Database.Storage.Voron.StorageActions
{
    using System.Collections.Generic;
    using System.IO;

    using Abstractions.Data;
    using Abstractions.Extensions;
    using Database.Impl;
    using Impl;
    using Raven.Json.Linq;

    using global::Voron;
    using global::Voron.Impl;

    internal class ListsStorageActions : StorageActionsBase, IListsStorageActions
    {
        private readonly TableStorage tableStorage;

        private readonly IUuidGenerator generator;

        private readonly Reference<WriteBatch> writeBatch;
        private readonly GeneralStorageActions generalStorageActions;

        private static readonly ILog Logger = LogManager.GetCurrentClassLogger();

        public ListsStorageActions(TableStorage tableStorage,
            IUuidGenerator generator, Reference<SnapshotReader> snapshot,
            Reference<WriteBatch> writeBatch,
            IBufferPool bufferPool,
            GeneralStorageActions generalStorageActions)
            : base(snapshot, bufferPool)
        {
            this.tableStorage = tableStorage;
            this.generator = generator;
            this.writeBatch = writeBatch;
            this.generalStorageActions = generalStorageActions;
        }

        public Etag Set(string name, string key, RavenJObject data, UuidType type)
        {
            var listsByName = tableStorage.Lists.GetIndex(Tables.Lists.Indices.ByName);
            var listsByNameAndKey = tableStorage.Lists.GetIndex(Tables.Lists.Indices.ByNameAndKey);

            var nameKey = CreateKey(name);
            var nameKeySlice = (Slice)nameKey;
            var nameAndKeySlice = (Slice)AppendToKey(nameKey, key);

            string existingEtag = null;
            bool update = false;

            var read = listsByNameAndKey.Read(Snapshot, nameAndKeySlice, writeBatch.Value);
            if (read != null)
            {
                update = true;

                using (var stream = read.Reader.AsStream())
                {
                    using (var reader = new StreamReader(stream))
                        existingEtag = reader.ReadToEnd();
                }
            }

            var etag = generator.CreateSequentialUuid(type);
            var internalKey = update == false ? etag.ToString() : existingEtag;
            var internalKeyAsSlice = (Slice)internalKey;
            var createdAt = SystemTime.UtcNow;

            tableStorage.Lists.Add(
                writeBatch.Value,
                internalKeyAsSlice,
                new RavenJObject
                {
                    { "name", name },
                    { "key", key },
                    { "etag", etag.ToByteArray() },
                    { "data", data },
                    { "createdAt", createdAt}
                });

            if (update == false)
            {
                listsByName.MultiAdd(writeBatch.Value, nameKeySlice, internalKeyAsSlice);
                listsByNameAndKey.Add(writeBatch.Value, nameAndKeySlice, internalKey);
            }

            return etag;
        }

        public void Remove(string name, string key)
        {
            var listsByName = tableStorage.Lists.GetIndex(Tables.Lists.Indices.ByName);
            var listsByNameAndKey = tableStorage.Lists.GetIndex(Tables.Lists.Indices.ByNameAndKey);

            var nameKey = CreateKey(name);
            var nameKeySlice = (Slice)nameKey;
            var nameAndKeySlice = (Slice)AppendToKey(nameKey, key);

            var read = listsByNameAndKey.Read(Snapshot, nameAndKeySlice, writeBatch.Value);
            if (read == null)
                return;

            using (var stream = read.Reader.AsStream())
            {
                using (var reader = new StreamReader(stream))
                {
                    var etag = (Slice)reader.ReadToEnd();

                    tableStorage.Lists.Delete(writeBatch.Value, etag);
                    listsByName.MultiDelete(writeBatch.Value, nameKeySlice, etag);
                    listsByNameAndKey.Delete(writeBatch.Value, nameAndKeySlice);
                }
            }
        }

        public IEnumerable<ListItem> Read(string name, Etag start, Etag end, int take)
        {
            var listsByName = tableStorage.Lists.GetIndex(Tables.Lists.Indices.ByName);

            using (var iterator = listsByName.MultiRead(Snapshot, (Slice)CreateKey(name)))
            {
                if (!iterator.Seek((Slice)start.ToString()))
                    yield break;

                int count = 0;

                do
                {
                    var etag = Etag.Parse(iterator.CurrentKey.ToString());
                    if (start.CompareTo(etag) >= 0)
                        continue;

                    if (end != null && end.CompareTo(etag) <= 0)
                        yield break;

                    count++;
                    yield return ReadInternal(etag);
                }
                while (iterator.MoveNext() && count < take);
            }
        }

        public IEnumerable<ListItem> Read(string name, int start, int take)
        {
            var listsByName = tableStorage.Lists.GetIndex(Tables.Lists.Indices.ByName);

            using (var iterator = listsByName.MultiRead(Snapshot, (Slice)CreateKey(name)))
            {
                if (!iterator.Seek(Slice.BeforeAllKeys))
                    yield break;

                int count = 0;

                int skipped = 0;
                while (skipped < start)
                {
                    if (!iterator.MoveNext())
                        yield break;
                    skipped++;
                }

                do
                {
                    var etag = Etag.Parse(iterator.CurrentKey.ToString());

                    count++;
                    yield return ReadInternal(etag);
                }
                while (iterator.MoveNext() && count < take);
            }
        }

        public ListItem Read(string name, string key)
        {
            var listsByNameAndKey = tableStorage.Lists.GetIndex(Tables.Lists.Indices.ByNameAndKey);
            var nameAndKey = (Slice)CreateKey(name, key);

            var read = listsByNameAndKey.Read(Snapshot, nameAndKey, writeBatch.Value);
            if (read == null)
                return null;

            using (var stream = read.Reader.AsStream())
            {
                using (var reader = new StreamReader(stream))
                {
                    var etag = reader.ReadToEnd();
                    return ReadInternal(etag);
                }
            }
        }

        public ListItem ReadLast(string name)
        {
            var listsByName = tableStorage.Lists.GetIndex(Tables.Lists.Indices.ByName);
            var nameKey = (Slice)CreateKey(name);

            using (var iterator = listsByName.MultiRead(Snapshot, nameKey))
            {
                if (!iterator.Seek(Slice.AfterAllKeys))
                    return null;

                var etag = Etag.Parse(iterator.CurrentKey.ToString());

                return ReadInternal(etag);
            }
        }

        public void RemoveAllBefore(string name, Etag etag, TimeSpan? timeout = null)
        {
            var listsByName = tableStorage.Lists.GetIndex(Tables.Lists.Indices.ByName);
            var listsByNameAndKey = tableStorage.Lists.GetIndex(Tables.Lists.Indices.ByNameAndKey);

            var nameKey = CreateKey(name);
            var nameKeySlice = (Slice)nameKey;

            var iterator = listsByName.MultiRead(Snapshot, nameKeySlice);
            try
            {
                if (!iterator.Seek(Slice.BeforeAllKeys))
                    return;
                bool skipMoveNext;

                Stopwatch duration = null;
                if (timeout != null)
                    duration = Stopwatch.StartNew();

                do
                {
                    skipMoveNext = false;
                    var currentEtag = Etag.Parse(iterator.CurrentKey.ToString());

                    if (currentEtag.CompareTo(etag) > 0)
                        break;

                    if(timeout != null && duration.Elapsed > timeout.Value)
                        break;
                        
                    ushort version;
                    var value = LoadJson(tableStorage.Lists, iterator.CurrentKey, writeBatch.Value, out version);

                    var key = value.Value<string>("key");
                    var etagSlice = (Slice)currentEtag.ToString();

                    tableStorage.Lists.Delete(writeBatch.Value, etagSlice);
                    listsByName.MultiDelete(writeBatch.Value, nameKeySlice, etagSlice);
                    listsByNameAndKey.Delete(writeBatch.Value, (Slice)AppendToKey(nameKey, key));

                    if (generalStorageActions.MaybePulseTransaction(iterator))
                    {
                        iterator = listsByName.MultiRead(Snapshot, nameKeySlice);
                        if (!iterator.Seek(Slice.BeforeAllKeys))
                            break;
                        skipMoveNext = true;
                    }
                } while (skipMoveNext || iterator.MoveNext());
            }
            finally
            {
                if (iterator != null)
                    iterator.Dispose();
            }
        }

        public void RemoveAllOlderThan(string name, DateTime cutoff)
        {
            var listsByName = tableStorage.Lists.GetIndex(Tables.Lists.Indices.ByName);
            var listsByNameAndKey = tableStorage.Lists.GetIndex(Tables.Lists.Indices.ByNameAndKey);

            var nameKey = CreateKey(name);
            var nameKeySlice = (Slice)nameKey;

            var iterator = listsByName.MultiRead(Snapshot, nameKeySlice);
            try
            {
                if (!iterator.Seek(Slice.BeforeAllKeys))
                    return;

                bool skipMoveNext;
                do
                {
                    skipMoveNext = false;
                    ushort version;
                    var value = LoadJson(tableStorage.Lists, iterator.CurrentKey, writeBatch.Value, out version);
                    if (value == null)
                    {
                        Logger.Warn("Couldn't locate key : '{0}' in Lists TableStorage for '{1}'",
                            iterator.CurrentKey, name);
                        continue;
                    }

                    var createdAt = value.Value<DateTime>("createdAt");
                    if (createdAt > cutoff)
                        break;

                    var key = value.Value<string>("key");
                    var etag = Etag.Parse(iterator.CurrentKey.ToString());
                    var etagSlice = (Slice)etag.ToString();

                    tableStorage.Lists.Delete(writeBatch.Value, etagSlice);
                    listsByName.MultiDelete(writeBatch.Value, nameKeySlice, etagSlice);
                    listsByNameAndKey.Delete(writeBatch.Value, AppendToKey(nameKey, key));

                    if (generalStorageActions.MaybePulseTransaction(iterator))
                    {
                        iterator = listsByName.MultiRead(Snapshot, nameKeySlice);
                        if (!iterator.Seek(Slice.BeforeAllKeys))
                            break;
                        skipMoveNext = true;
                    }
                } while (skipMoveNext || iterator.MoveNext());
            }
            finally
            {
                if (iterator != null)
                    iterator.Dispose();
            }
        }

        public void Touch(string name, string key, UuidType uuidType, out Etag preTouchEtag, out Etag afterTouchEtag)
        {
            var item = Read(name, key);
            if (item == null)
            {
                afterTouchEtag = null;
                preTouchEtag = null;
                return;
            }

            preTouchEtag = item.Etag;

            Remove(name, key);

            afterTouchEtag = generator.CreateSequentialUuid(uuidType);
            var internalKey = afterTouchEtag.ToString();
            var internalKeyAsSlice = (Slice)internalKey;

            tableStorage.Lists.Add(
                writeBatch.Value,
                internalKeyAsSlice,
                new RavenJObject
                {
                    {"name", name},
                    {"key", key},
                    {"etag", afterTouchEtag.ToByteArray()},
                    {"data", item.Data},
                    {"createdAt", item.CreatedAt}
                });

            var listsByName = tableStorage.Lists.GetIndex(Tables.Lists.Indices.ByName);
            var listsByNameAndKey = tableStorage.Lists.GetIndex(Tables.Lists.Indices.ByNameAndKey);

            var nameKey = CreateKey(name);
            var nameKeySlice = (Slice)nameKey;
            var nameAndKeySlice = (Slice)AppendToKey(nameKey, key);

            listsByName.MultiAdd(writeBatch.Value, nameKeySlice, internalKeyAsSlice);
            listsByNameAndKey.Add(writeBatch.Value, nameAndKeySlice, internalKey);
        }

        public List<ListsInfo> GetListsStatsVerySlowly()
        {
            string currentName = null;
            List<ListsInfo> res = new List<ListsInfo>();
            ListsInfo currentListsInfo = null;
            var listsByName = tableStorage.Lists.GetIndex(Tables.Lists.Indices.ByName);
            using (var iterator = listsByName.Iterate(Snapshot, writeBatch.Value))
            {
                if (!iterator.Seek(Slice.BeforeAllKeys))
                {
                    return res;
                }
                do
                {
                    currentListsInfo = new ListsInfo() { Name = iterator.CurrentKey.ToString() };
                    using (var internalIterator = listsByName.MultiRead(Snapshot, iterator.CurrentKey))
                    {
                        if (!internalIterator.Seek(Slice.BeforeAllKeys))
                        {
                            continue;
                        }
                        do
                        {
                            var sizeOnDisk = iterator.GetCurrentDataSize();
                            currentListsInfo.Count++;
                            currentListsInfo.SizeOnDiskInBytes += sizeOnDisk;
                            if (sizeOnDisk > currentListsInfo.MaxListItemSizeOnDiskInBytes)
                                currentListsInfo.MaxListItemSizeOnDiskInBytes = sizeOnDisk;
                            if (currentListsInfo.MinListItemSizeOnDiskInBytes == 0 || sizeOnDisk < currentListsInfo.MinListItemSizeOnDiskInBytes)
                                currentListsInfo.MinListItemSizeOnDiskInBytes = sizeOnDisk;
                        } while (internalIterator.MoveNext());
                    }
                    res.Add(currentListsInfo);
                    currentListsInfo.AverageListItemSizeOnDiskInBytes = currentListsInfo.SizeOnDiskInBytes / currentListsInfo.Count;
                } while (iterator.MoveNext());
            }
            res.Sort((a, b) => b.SizeOnDiskInBytes.CompareTo(a.SizeOnDiskInBytes));
            return res;
        }

        private ListItem ReadInternal(string id)
        {
            ushort version;
            var value = LoadJson(tableStorage.Lists, (Slice)id, writeBatch.Value, out version);
            if (value == null)
                return null;

            var etag = Etag.Parse(value.Value<byte[]>("etag"));
            var key = value.Value<string>("key");
            var createdAt = value.Value<DateTime>("createdAt");

            return new ListItem
            {
                Data = value.Value<RavenJObject>("data"),
                Etag = etag,
                Key = key,
                CreatedAt = createdAt
            };
        }
    }
}