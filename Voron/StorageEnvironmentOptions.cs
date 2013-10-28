using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using Voron.Impl;

namespace Voron
{
    public abstract class StorageEnvironmentOptions : IDisposable
    {
        public long LogFileSize { get; set; }

        public bool OwnsPagers { get; set; }

        public bool DeleteUnusedLogFiles { get; set; }

        public abstract IVirtualPager DataPager { get; }

        public abstract IVirtualPager CreateLogPager(string name);

        protected StorageEnvironmentOptions()
        {
            LogFileSize = 64 * 1024 * 1024;
            OwnsPagers = true;
            DeleteUnusedLogFiles = true;
        }

        public static StorageEnvironmentOptions GetInMemory()
        {
            return new PureMemoryStorageEnvironmentOptions();
        }

        public static StorageEnvironmentOptions ForPath(string path, FlushMode flushMode = FlushMode.Full)
        {
            return new DirectoryStorageEnvironmentOptions(path, flushMode);
        }

        public class DirectoryStorageEnvironmentOptions : StorageEnvironmentOptions
        {
            private readonly FlushMode _flushMode;
            private readonly string _basePath;
            private readonly Lazy<IVirtualPager> _dataPager;

            private readonly ConcurrentDictionary<string, Lazy<IVirtualPager>> _journals =
                new ConcurrentDictionary<string, Lazy<IVirtualPager>>(StringComparer.OrdinalIgnoreCase);

            public DirectoryStorageEnvironmentOptions(string basePath, FlushMode flushMode)
            {
                _flushMode = flushMode;
                _basePath = Path.GetFullPath(basePath);
                _dataPager = new Lazy<IVirtualPager>(CreateDataPager);
            }

            private IVirtualPager CreateDataPager()
            {
                if (Directory.Exists(_basePath) == false)
                {
                    Directory.CreateDirectory(_basePath);
                }
                return new MemoryMapPager(Path.Combine(_basePath, "db.voron"), _flushMode);
            }

            public override IVirtualPager DataPager
            {
                get
                {
                    return _dataPager.Value;
                }
            }

            public override IVirtualPager CreateLogPager(string name)
            {
                var path = Path.Combine(_basePath, name);
                var orAdd = _journals.GetOrAdd(name, _ => new Lazy<IVirtualPager>(() => new MemoryMapPager(path, _flushMode)));
                return orAdd.Value;
            }

            public override void Dispose()
            {
                if(_dataPager.IsValueCreated)
                    _dataPager.Value.Dispose();
                foreach (var journal in _journals)
                {
                    if(journal.Value.IsValueCreated)
                        journal.Value.Value.Dispose();
                }
            }
        }

        public class PureMemoryStorageEnvironmentOptions : StorageEnvironmentOptions
        {
            private readonly PureMemoryPager _dataPager;

            private Dictionary<string, IVirtualPager> _logs =
                new Dictionary<string, IVirtualPager>(StringComparer.OrdinalIgnoreCase);

            public PureMemoryStorageEnvironmentOptions()
            {
                _dataPager = new PureMemoryPager();
            }

            public override IVirtualPager DataPager
            {
                get { return _dataPager; }
            }

            public override IVirtualPager CreateLogPager(string name)
            {
                IVirtualPager value;
                if (_logs.TryGetValue(name, out value))
                    return value;
                value = new PureMemoryPager();
                _logs[name] = value;
                return value;
            }

            public override void Dispose()
            {
                _dataPager.Dispose();
                foreach (var virtualPager in _logs)
                {
                    virtualPager.Value.Dispose();
                }
            }
        }

        public abstract void Dispose();
    }

}