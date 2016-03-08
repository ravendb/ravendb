// -----------------------------------------------------------------------
//  <copyright file="ConcurrentOrderedList.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;

namespace Raven.Database.Prefetching
{
    public class ConcurrentJsonDocumentSortedList
    {
        private readonly ReaderWriterLockSlim slim = new ReaderWriterLockSlim();

        private readonly SortedList<Etag, JsonDocument> innerList;

        private int loadedSize;

        public ConcurrentJsonDocumentSortedList()
        {
            innerList = new SortedList<Etag, JsonDocument>();
        }


        public SortedList<Etag, JsonDocument> Clone()
        {
            try
            {
                slim.EnterReadLock();
                return new SortedList<Etag, JsonDocument>(innerList);
            }
            finally
            {
                slim.ExitReadLock();
            }
        }

        public int Count
        {
            get
            {
                try
                {
                    slim.EnterReadLock();
                    return innerList.Count;
                }
                finally
                {
                    slim.ExitReadLock();
                }
            }
        }

        public IDisposable EnterWriteLock()
        {
            slim.EnterWriteLock();
            return new DisposableAction(slim.ExitWriteLock);
        }

        public void Add(JsonDocument value)
        {
            Debug.Assert(slim.IsWriteLockHeld);

            innerList[value.Etag] = value;

            loadedSize += value.SerializedSizeOnDisk;
        }

        public bool TryPeek(out JsonDocument result)
        {
            try
            {
                slim.EnterReadLock();

                result = innerList.Values.FirstOrDefault();

                return result != null;
            }
            finally
            {
                slim.ExitReadLock();
            }
        }

        public bool TryPeekLastDocument(out JsonDocument result)
        {
            try
            {
                slim.EnterReadLock();

                result = innerList.Values.LastOrDefault();

                return result != null;
            }
            finally
            {
                slim.ExitReadLock();
            }
        }

        public bool TryDequeue(out JsonDocument result)
        {
            try
            {
                slim.EnterWriteLock();
                result = innerList.Values.FirstOrDefault();
                if (result != null)
                {
                    innerList.RemoveAt(0);
                    loadedSize -= result.SerializedSizeOnDisk;
                }

                return result != null;
            }
            finally
            {
                slim.ExitWriteLock();
            }
        }

        public Etag NextDocumentETag()
        {
            JsonDocument result;
            return TryPeek(out result) == false ? null : result.Etag;
        }

        public int LoadedSize
        {
            get
            {
                slim.EnterReadLock();
                try
                {
                    return loadedSize;
                }
                finally
                {
                    slim.ExitReadLock();
                }
            }
        }

        public void RemoveAfter(Etag etag)
        {
            slim.EnterWriteLock();
            try
            {
                for (var i = innerList.Count - 1; i >= 0; i--)
                {
                    var doc = innerList.Values[i];
                    if (doc.Etag.CompareTo(etag) < 0)
                        break;

                    innerList.RemoveAt(i);
                    loadedSize -= doc.SerializedSizeOnDisk;
                }
            }
            finally
            {
                slim.ExitWriteLock();
            }
        }

        public void Clear()
        {
            slim.EnterWriteLock();
            try
            {
                innerList.Clear();
                loadedSize = 0;
            }
            finally
            {
                slim.ExitWriteLock();
            }
        }

        public bool DocumentExists(Etag etag)
        {
            try
            {
                slim.EnterReadLock();

                if (innerList.Count == 0)
                    return false;

                JsonDocument _;
                return innerList.TryGetValue(etag, out _); //o(log n)
            }
            finally
            {
                slim.ExitReadLock();
            }
        }
    }
}
