using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Util;

namespace Raven.Client.Documents.Session
{
    public abstract class ClusterTransactionOperationsBase
    {
        private readonly InMemoryDocumentSessionOperations _session;

        public class StoredCompareExchange
        {
            public readonly object Entity;
            public readonly long Index;
            
            public StoredCompareExchange(long index, object entity)
            {
                Entity = entity;
                Index = index;
            }
        }

        private Dictionary<string, StoredCompareExchange> _storeCompareExchange;
        public Dictionary<string, StoredCompareExchange> StoreCompareExchange => _storeCompareExchange;

        private Dictionary<string, long> _deleteCompareExchange;
        public Dictionary<string, long> DeleteCompareExchange => _deleteCompareExchange;

        protected ClusterTransactionOperationsBase(InMemoryDocumentSessionOperations session)
        {
            if (session.TransactionMode != TransactionMode.ClusterWide)
            {
                throw new InvalidOperationException($"This function is part of cluster transaction session, in order to use it you have to open the Session with '{nameof(TransactionMode.ClusterWide)}' option.");
            }

            _session = session;
        }

        public void CreateCompareExchangeValue<T>(string key, T item)
        {
            if (_storeCompareExchange == null)
                _storeCompareExchange = new Dictionary<string, StoredCompareExchange>();

            EnsureNotDeleted(key);
            EnsureNotStored(key);

            _storeCompareExchange[key] = new StoredCompareExchange(0, item);
        }

        public void UpdateCompareExchangeValue<T>(CompareExchangeValue<T> item)
        {
            EnsureNotDeleted(item.Key);

            if (_storeCompareExchange == null)
                _storeCompareExchange = new Dictionary<string, StoredCompareExchange>();

            _storeCompareExchange[item.Key] = new StoredCompareExchange(item.Index, item.Value);
        }

        public void DeleteCompareExchangeValue<T>(CompareExchangeValue<T> item)
        {
            EnsureNotStored(item.Key);

            if (_deleteCompareExchange == null)
                _deleteCompareExchange = new Dictionary<string, long>();

            _deleteCompareExchange[item.Key] = item.Index;
        }

        public void DeleteCompareExchangeValue(string key, long index)
        {
            EnsureNotStored(key);

            if (_deleteCompareExchange == null)
                _deleteCompareExchange = new Dictionary<string, long>();

            _deleteCompareExchange[key] = index;
        }

        public void Clear()
        {
            _deleteCompareExchange = null;
            _storeCompareExchange = null;
        }

        public bool HasCommands => _deleteCompareExchange != null || _storeCompareExchange != null;

        protected Task<CompareExchangeValue<T>> GetCompareExchangeValueAsyncInternal<T>(string key)
        {
            return _session.Operations.SendAsync(new GetCompareExchangeValueOperation<T>(key));
        }

        protected Task<Dictionary<string, CompareExchangeValue<T>>> GetCompareExchangeValuesInternal<T>(string[] keys)
        {
            return _session.Operations.SendAsync(new GetCompareExchangeValuesOperation<T>(keys));
        }

        protected void EnsureNotDeleted(string key)
        {
            if (_deleteCompareExchange?.ContainsKey(key) == true)
            {
                throw new ArgumentException($"The key '{key}' already exists in the deletion requests.");
            }
        }

        protected void EnsureNotStored(string key)
        {
            if (_storeCompareExchange?.ContainsKey(key) == true)
            {
                throw new ArgumentException($"The key '{key}' already exists in the store requests.");
            }
        }
    }

    public interface IClusterTransactionOperationsBase
    {
        void DeleteCompareExchangeValue(string key, long index);

        void DeleteCompareExchangeValue<T>(CompareExchangeValue<T> item);

        void UpdateCompareExchangeValue<T>(CompareExchangeValue<T> item);

        void CreateCompareExchangeValue<T>(string key, T value);
    }

    public interface IClusterTransactionOperations : IClusterTransactionOperationsBase
    {
        CompareExchangeValue<T> GetCompareExchangeValue<T>(string key);

        Dictionary<string, CompareExchangeValue<T>> GetCompareExchangeValues<T>(string[] keys);
    }

    public interface IClusterTransactionOperationsAsync : IClusterTransactionOperationsBase
    {
        Task<CompareExchangeValue<T>> GetCompareExchangeValueAsync<T>(string key);

        Task<Dictionary<string, CompareExchangeValue<T>>> GetCompareExchangeValuesAsync<T>(string[] keys);
    }

    public class ClusterTransactionOperationsAsync : ClusterTransactionOperationsBase, IClusterTransactionOperationsAsync
    {
        public ClusterTransactionOperationsAsync(InMemoryDocumentSessionOperations session) : base(session)
        {
        }

        public Task<CompareExchangeValue<T>> GetCompareExchangeValueAsync<T>(string key)
        {
            return GetCompareExchangeValueAsyncInternal<T>(key);
        }

        public Task<Dictionary<string, CompareExchangeValue<T>>> GetCompareExchangeValuesAsync<T>(string[] keys)
        {
            return GetCompareExchangeValuesInternal<T>(keys);
        }
    }

    public class ClusterTransactionOperations : ClusterTransactionOperationsBase, IClusterTransactionOperations
    {
        public ClusterTransactionOperations(InMemoryDocumentSessionOperations session) : base(session)
        {
        }

        public CompareExchangeValue<T> GetCompareExchangeValue<T>(string key)
        {
            return AsyncHelpers.RunSync(() => GetCompareExchangeValueAsyncInternal<T>(key));
        }

        public Dictionary<string, CompareExchangeValue<T>> GetCompareExchangeValues<T>(string[] keys)
        {
            return AsyncHelpers.RunSync(() => GetCompareExchangeValuesInternal<T>(keys));
        }
    }

    public class ClusterTransactionException : Exception
    {
        public ClusterTransactionException()
        {
        }

        public ClusterTransactionException(string message) : base(message)
        {
        }

        public ClusterTransactionException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
