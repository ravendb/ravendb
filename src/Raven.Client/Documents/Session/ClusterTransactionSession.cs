using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Util;

namespace Raven.Client.Documents.Session
{
    public abstract class ClusterTransactionSessionBase
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

        protected ClusterTransactionSessionBase(InMemoryDocumentSessionOperations session)
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

            _storeCompareExchange.Add(key, new StoredCompareExchange(0, item));
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

            if (_deleteCompareExchange.ContainsKey(item.Key) == false)
                _deleteCompareExchange.Add(item.Key, item.Index);
        }

        public void DeleteCompareExchangeValue(string key, long index)
        {
            EnsureNotStored(key);

            if (_deleteCompareExchange == null)
                _deleteCompareExchange = new Dictionary<string, long>();
            
            if (_deleteCompareExchange.ContainsKey(key) == false)
                _deleteCompareExchange.Add(key, index);
        }

        public void Clear()
        {
            _deleteCompareExchange?.Clear();
            _storeCompareExchange?.Clear();
            _deleteCompareExchange = null;
            _storeCompareExchange = null;
        }

        protected Task<CompareExchangeValue<T>> GetCompareExchangeValueAsyncInternal<T>(string key)
        {
            return _session.Operations.SendAsync(new GetCompareExchangeValueOperation<T>(key));
        }

        protected Task<Dictionary<string, long>> GetCompareExchangeIndexesInternal(string[] keys)
        {
            return _session.Operations.SendAsync(new GetCompareExchangeIndexOperation(keys));
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

    public interface IClusterTransactionOperationBase
    {
        void DeleteCompareExchangeValue(string key, long index);

        void DeleteCompareExchangeValue<T>(CompareExchangeValue<T> item);

        void UpdateCompareExchangeValue<T>(CompareExchangeValue<T> item);

        void CreateCompareExchangeValue<T>(string key, T value);
    }

    public interface IClusterTransactionOperation : IClusterTransactionOperationBase
    {
        CompareExchangeValue<T> GetCompareExchangeValue<T>(string key);

        Dictionary<string, long> GetCompareExchangeIndexes(string[] keys);
    }

    public interface IClusterTransactionOperationAsync : IClusterTransactionOperationBase
    {
        Task<CompareExchangeValue<T>> GetCompareExchangeValueAsync<T>(string key);

        Task<Dictionary<string, long>> GetCompareExchangeIndexesAsync(string[] keys);
    }

    public class ClusterTransactionTransactionOperationAsync : ClusterTransactionSessionBase, IClusterTransactionOperationAsync
    {
        public ClusterTransactionTransactionOperationAsync(InMemoryDocumentSessionOperations session) : base(session)
        {
        }

        public Task<CompareExchangeValue<T>> GetCompareExchangeValueAsync<T>(string key)
        {
            return GetCompareExchangeValueAsyncInternal<T>(key);
        }

        public Task<Dictionary<string, long>> GetCompareExchangeIndexesAsync(string[] keys)
        {
            return GetCompareExchangeIndexesInternal(keys);
        }
    }

    public class ClusterTransactionTransactionOperation : ClusterTransactionSessionBase, IClusterTransactionOperation
    {
        public ClusterTransactionTransactionOperation(InMemoryDocumentSessionOperations session) : base(session)
        {
        }

        public CompareExchangeValue<T> GetCompareExchangeValue<T>(string key)
        {
            return AsyncHelpers.RunSync(() => GetCompareExchangeValueAsyncInternal<T>(key));
        }

        public Dictionary<string, long> GetCompareExchangeIndexes(string[] keys)
        {
            return AsyncHelpers.RunSync(() => GetCompareExchangeIndexesInternal(keys));
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
