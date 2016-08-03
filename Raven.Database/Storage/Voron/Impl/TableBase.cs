// -----------------------------------------------------------------------
//  <copyright file="TableBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util.Streams;
using Raven.Database.Util.Streams;
using Raven.Json.Linq;

using Voron;
using Voron.Impl;
using Voron.Impl.Paging;
using Voron.Trees;

namespace Raven.Database.Storage.Voron.Impl
{
    internal abstract class TableBase
    {
        protected IBufferPool BufferPool { get; private set; }

        public string TableName { get; private set; }

        protected TableBase(string tableName, IBufferPool bufferPool)
        {
            if (string.IsNullOrEmpty(tableName))
                throw new ArgumentNullException(tableName);

            BufferPool = bufferPool;
            TableName = tableName;
        }

        public virtual void Add(WriteBatch writeBatch, string key, string value, ushort? expectedVersion = null)
        {
            Add(writeBatch, key, Encoding.UTF8.GetBytes(value), expectedVersion);
        }

        public virtual void Add(WriteBatch writeBatch, string key, byte[] value, ushort? expectedVersion = null)
        {
            Add(writeBatch, (Slice) key, value, expectedVersion);
        }

        public virtual void Add(WriteBatch writeBatch, Slice key, Stream value, ushort? expectedVersion = null, bool shouldIgnoreConcurrencyExceptions = false)
        {
            AssertKey(key);

            writeBatch.Add(key, value, TableName, expectedVersion, shouldIgnoreConcurrencyExceptions);
        }

        public virtual void Add(WriteBatch writeBatch, Slice key, RavenJToken value, ushort? expectedVersion = null)
        {
            AssertKey(key);

            var stream = new BufferPoolMemoryStream();
            value.WriteTo(stream);
            stream.Position = 0;

            writeBatch.Add(key, stream, TableName, expectedVersion);
        }

        public virtual void Add(WriteBatch writeBatch, Slice key, string value, ushort? expectedVersion = null)
        {
            Add(writeBatch, key, Encoding.UTF8.GetBytes(value), expectedVersion);
        }

        public virtual void Add(WriteBatch writeBatch, Slice key, byte[] value, ushort? expectedVersion = null)
        {
            AssertKey(key);

            var stream = new BufferPoolMemoryStream();
            stream.Write(value, 0, value.Length);
            stream.Position = 0;

            writeBatch.Add(key, stream, TableName, expectedVersion);
        }

        public virtual void Increment(WriteBatch writeBatch, Slice key, long delta, ushort? expectedVersion = null)
        {
            AssertKey(key);

            writeBatch.Increment(key, delta, TableName, expectedVersion);
        }

        public virtual void MultiAdd(WriteBatch writeBatch, Slice key, Slice value, ushort? expectedVersion = null)
        {
            AssertKey(key);

            writeBatch.MultiAdd(key, value, TableName, expectedVersion);
        }

        public virtual ReadResult Read(SnapshotReader snapshot, Slice key, WriteBatch writeBatch)
        {
            return snapshot.Read(TableName, key, writeBatch);
        }

        public virtual IIterator MultiRead(SnapshotReader snapshot, Slice key)
        {
            return snapshot.MultiRead(TableName, key);
        }

        public virtual IIterator Iterate(SnapshotReader snapshot, WriteBatch writeBatch)
        {
            return snapshot.Iterate(TableName);
        }

        public bool Contains(SnapshotReader snapshot, Slice key, WriteBatch writeBatch)
        {
            ushort? version;
            return Contains(snapshot, key, writeBatch, out version);
        }

        public bool Contains(SnapshotReader snapshot, Slice key, WriteBatch writeBatch, out ushort? version)
        {
            return snapshot.Contains(TableName, key, out version, writeBatch);
        }

        public int GetDataSize(SnapshotReader snapshot, Slice key)
        {
            return snapshot.GetDataSize(TableName, key);
        }

        public virtual void Delete(WriteBatch writeBatch, string key, ushort? expectedVersion = null)
        {
            Delete(writeBatch, (Slice)key, expectedVersion);
        }

        public virtual void Delete(WriteBatch writeBatch, Slice key, ushort? expectedVersion = null, bool shouldIgnoreConcurrencyExceptions = false)
        {
            AssertKey(key);

            writeBatch.Delete(key, TableName, expectedVersion, shouldIgnoreConcurrencyExceptions);
        }

        public virtual void MultiDelete(WriteBatch writeBatch, Slice key, Slice value, ushort? expectedVersion = null)
        {
            AssertKey(key);

            writeBatch.MultiDelete(key, value, TableName, expectedVersion);
        }

        public virtual ushort? ReadVersion(SnapshotReader snapshot, Slice key, WriteBatch writeBatch)
        {
            return snapshot.ReadVersion(TableName, key, writeBatch);
        }

        protected void AssertKey(Slice key)
        {
            if (AbstractPager.IsKeySizeValid(key.Size, false) == false)
                throw new ArgumentException(string.Format("The key must be a maximum of {0} bytes in UTF8, key is: '{1}'", AbstractPager.GetMaxKeySize(false), key), "key");
        }

        //for debugging purposes
        public Dictionary<string, string> Dump(SnapshotReader snapshot)
        {
            using (var iterator = snapshot.Iterate(TableName))
            {
                if (!iterator.Seek(Slice.BeforeAllKeys))
                    return new Dictionary<string, string>();
                var results = new Dictionary<string, string>();
                do
                {
                    bool isMultiTreeKey;
                    using (var multiIterator = snapshot.MultiRead(TableName, iterator.CurrentKey))
                    {
                        if (!multiIterator.Seek(Slice.BeforeAllKeys))
                        {
                            isMultiTreeKey = false;
                        }
                        else
                        {
                            isMultiTreeKey = true;
                            const string subtreeKeyPrefix = "[sub tree val: ]";

                            do
                            {
                                results.Add(subtreeKeyPrefix + iterator.CurrentKey + " " + results.Count , new StreamReader(multiIterator.CreateReaderForCurrent().AsStream()).ReadToEnd());
                            } while (multiIterator.MoveNext());

                        }

                    }

                    if(!isMultiTreeKey)
                        results.Add(iterator.CurrentKey.ToString(), new StreamReader(iterator.CreateReaderForCurrent().AsStream()).ReadToEnd());

                } while (iterator.MoveNext());

                return results;
            }
        }
    }
}
