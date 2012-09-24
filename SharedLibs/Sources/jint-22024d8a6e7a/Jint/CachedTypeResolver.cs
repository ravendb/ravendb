using System;
using System.Collections.Generic;
using System.Threading;

namespace Jint {
    public class CachedTypeResolver : ITypeResolver {
        private readonly Dictionary<string, Type> _cache = new Dictionary<string, Type>();
        private readonly ReaderWriterLock _lock = new ReaderWriterLock();
        private static CachedTypeResolver _default;

        public static CachedTypeResolver Default {
            get {
                lock (typeof(CachedTypeResolver)) {
                    return _default ?? (_default = new CachedTypeResolver());
                }
            }
        }

        public Type ResolveType(string fullname) {
            _lock.AcquireReaderLock(Timeout.Infinite);

            try {
                if (_cache.ContainsKey(fullname)) {
                    return _cache[fullname];
                }

                Type type = null;
                foreach (var a in AppDomain.CurrentDomain.GetAssemblies()) {
                    type = a.GetType(fullname, false, false);

                    if (type != null) {
                        break;
                    }
                }

                _lock.UpgradeToWriterLock(Timeout.Infinite);

                _cache.Add(fullname, type);
                return type;

            }
            finally {
                _lock.ReleaseLock();
            }
        }
    }
}
