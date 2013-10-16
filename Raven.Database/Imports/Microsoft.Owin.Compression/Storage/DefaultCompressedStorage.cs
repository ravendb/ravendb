// <copyright file="DefaultCompressedStorage.cs" company="Microsoft Open Technologies, Inc.">
// Copyright 2011-2013 Microsoft Open Technologies, Inc. All rights reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// </copyright>

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Threading;

namespace Microsoft.Owin.Compression.Storage
{
    public sealed class DefaultCompressedStorage : ICompressedStorage
    {
        private readonly IDictionary<CompressedKey, ItemHandle> _items = new Dictionary<CompressedKey, ItemHandle>(CompressedKey.CompressedKeyComparer);
        private readonly object _itemsLock = new object();
        private string _storagePath;
        private FileStream _lockFile;

        public void Initialize()
        {
            // TODO: guard against many things, including re-execution or 
            string basePath = Path.Combine(Path.GetTempPath(), "MsOwinCompression");
            _storagePath = Path.Combine(basePath, Guid.NewGuid().ToString("n"));
            string lockPath = Path.Combine(_storagePath, "_lock");
            Directory.CreateDirectory(_storagePath);
            _lockFile = new FileStream(lockPath, FileMode.Create, FileAccess.Write, FileShare.None);

            ThreadPool.QueueUserWorkItem(_ => CleanupReleasedStorage(basePath));
        }

        public void Dispose()
        {
            // TODO: implement ~finalizer, etc

            ItemHandle[] items;
            lock (_itemsLock)
            {
                items = _items.Values.ToArray();
                _items.Clear();
            }

            var exceptions = new List<Exception>();
            foreach (var item in items)
            {
                try
                {
                    item.Dispose();
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            }

            try
            {
                _lockFile.Close();
                _lockFile = null;
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
            }

            try
            {
                Directory.Delete(_storagePath, true);
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
            }

            if (exceptions.Count != 0)
            {
                // TODO: Log, don't throw from dispose.
                Debug.Fail("Cleanup exceptions: " 
                    + exceptions.Select(ex => ex.ToString()).Aggregate((s1, s2) => s1 + "\r\n" + s2));
                // throw new AggregateException(exceptions);
            }
        }

        private void CleanupReleasedStorage(string basePath)
        {
            foreach (var directory in Directory.GetDirectories(basePath))
            {
                string directoryPath = Path.Combine(basePath, Path.GetFileName(directory));
                if (string.Equals(directoryPath, _storagePath, StringComparison.OrdinalIgnoreCase))
                {
                    // don't try to cleanup ourselves
                    continue;
                }
                string lockPath = Path.Combine(directoryPath, "_lock");
                if (File.Exists(lockPath))
                {
                    var lockInfo = new FileInfo(lockPath);
                    if (lockInfo.LastAccessTimeUtc > DateTime.UtcNow.Subtract(TimeSpan.FromHours(36)))
                    {
                        // less than a day and a half - don't try cleanup yet to avoid causing
                        // an exception if it's still in use
                        continue;
                    }
                    bool stillInUse = false;
                    try
                    {
                        File.Delete(lockPath);
                    }
                    catch
                    {
                        stillInUse = true;
                    }
                    if (stillInUse)
                    {
                        // can't delete - lock file still in use
                        continue;
                    }
                }
                Directory.Delete(directoryPath, true);
            }
        }

        public ICompressedItemHandle Open(CompressedKey key)
        {
            lock (_itemsLock)
            {
                ItemHandle handle;
                if (_items.TryGetValue(key, out handle))
                {
                    return handle.Clone();
                }
                return null;
            }
        }

        public ICompressedItemBuilder Create(CompressedKey key)
        {
            // TODO: break down into buckets to avoid files-per-folder limits
            string physicalPath = Path.Combine(_storagePath, Guid.NewGuid().ToString("n"));
            return new ItemBuilder(key, physicalPath);
        }

        [SuppressMessage("Microsoft.Reliability", "CA2000:DisposeObjectsBeforeLosingScope", Justification = "False positive")]
        public ICompressedItemHandle Commit(ICompressedItemBuilder builder)
        {
            var itemBuilder = (ItemBuilder)builder;
            CompressedKey key = itemBuilder.Key;
            var item = new Item
            {
                PhysicalPath = itemBuilder.PhysicalPath,
                CompressedLength = itemBuilder.Stream.Length
            };
            itemBuilder.Stream.Close();

            var handle = new ItemHandle(item);
            AddHandleInDictionary(key, handle);
            return handle;
        }

        private void AddHandleInDictionary(CompressedKey key, ItemHandle handle)
        {
            lock (_itemsLock)
            {
                ItemHandle addingHandle = handle.Clone();
                ItemHandle existingHandle;
                if (_items.TryGetValue(key, out existingHandle))
                {
                    existingHandle.Dispose();
                }
                _items[key] = addingHandle;
            }
        }

        private class ItemBuilder : ICompressedItemBuilder
        {
            public ItemBuilder(CompressedKey key, string physicalPath)
            {
                Key = key;
                PhysicalPath = physicalPath;
                Stream = new FileStream(PhysicalPath, FileMode.Create, FileAccess.Write, FileShare.None);
            }

            public CompressedKey Key { get; private set; }
            public string PhysicalPath { get; private set; }
            public Stream Stream { get; private set; }
        }

        private class Item
        {
            private int _references;

            public string PhysicalPath { get; set; }

            public long CompressedLength { get; set; }

            public void AddReference()
            {
                Interlocked.Increment(ref _references);
            }

            public void Release()
            {
                if (Interlocked.Decrement(ref _references) == 0)
                {
                    if (File.Exists(PhysicalPath))
                    {
                        File.Delete(PhysicalPath);
                    }
                }
            }
        }

        private class ItemHandle : ICompressedItemHandle
        {
            private Item _item;
            private bool _disposed;

            public ItemHandle(Item item)
            {
                item.AddReference();
                _item = item;
            }

            ~ItemHandle()
            {
                Dispose(false);
            }

            public string PhysicalPath
            {
                get { return _item.PhysicalPath; }
            }

            public long CompressedLength
            {
                get { return _item.CompressedLength; }
            }

            public ItemHandle Clone()
            {
                return new ItemHandle(_item);
            }

            public void Dispose()
            {
                Dispose(true);
                GC.SuppressFinalize(this);
            }

            private void Dispose(bool disposing)
            {
                if (!_disposed)
                {
                    Item item = Interlocked.Exchange(ref _item, null);
                    if (item != null)
                    {
                        item.Release();
                    }
                    _disposed = true;
                }
            }
        }
    }
}
