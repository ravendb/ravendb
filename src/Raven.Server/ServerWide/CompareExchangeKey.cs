using Sparrow.Json;

namespace Raven.Server.ServerWide
{
    public struct CompareExchangeKey
    {
        public readonly LazyStringValue StorageKey;

        public string Key
        {
            get
            {
                if (_key == null)
                    _key = StorageKey.Substring(_prefixLength);

                return _key;
            }
        }

        private string _key;

        private readonly int _prefixLength;

        public CompareExchangeKey(string key)
        {
            StorageKey = null;
            _key = key ?? throw new System.ArgumentNullException(nameof(key));
            _prefixLength = 0;
        }

        public CompareExchangeKey(LazyStringValue storageKey, string key)
        {
            StorageKey = storageKey ?? throw new System.ArgumentNullException(nameof(storageKey));
            _key = key ?? throw new System.ArgumentNullException(nameof(key));
            _prefixLength = 0;
        }

        public CompareExchangeKey(LazyStringValue storageKey, int prefixLength)
        {
            StorageKey = storageKey ?? throw new System.ArgumentNullException(nameof(storageKey));
            _key = null;
            _prefixLength = prefixLength;
        }

        public static string GetStorageKey(string database, string key)
        {
            return (database + "/" + key).ToLowerInvariant();
        }
    }
}
