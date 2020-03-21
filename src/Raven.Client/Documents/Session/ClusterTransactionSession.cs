using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session.Operations.Lazy;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    public abstract class ClusterTransactionOperationsBase<TSession> : ClusterTransactionOperationsBase
        where TSession : InMemoryDocumentSessionOperations
    {
        protected ClusterTransactionOperationsBase(TSession session) : base(session)
        {
            Session = session;
        }

        protected TSession Session { get; }
    }

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

        internal bool HasCommands => _deleteCompareExchange != null || _storeCompareExchange != null;

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

        protected async Task<CompareExchangeValue<T>> GetCompareExchangeValueAsyncInternal<T>(string key, CancellationToken token = default)
        {
            if (TryGetCompareExchangeValueFromSession(key, out var sessionValue))
                return sessionValue.GetValue<T>(_session.Conventions);

            using (_session.AsyncTaskHolder())
            {
                var value = await _session.Operations.SendAsync(new GetCompareExchangeValueOperation<BlittableJsonReaderObject>(key), sessionInfo: _session.SessionInfo, token: token).ConfigureAwait(false);

            }
        }

        protected async Task<Dictionary<string, CompareExchangeValue<T>>> GetCompareExchangeValuesAsyncInternal<T>(string[] keys, CancellationToken token = default)
        {
            using (_session.AsyncTaskHolder())
            {
                return await _session.Operations.SendAsync(new GetCompareExchangeValuesOperation<T>(keys), sessionInfo: _session.SessionInfo, token: token).ConfigureAwait(false);
            }
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

        internal void RegisterCompareExchangeValues(BlittableJsonReaderObject values)
        {
            if (_session.NoTracking || values == null)
                return;

            var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
            for (var i = 0; i < values.Count; i++)
            {
                values.GetPropertyByIndex(i, ref propertyDetails);

                RegisterCompareExchangeValue(propertyDetails.Value as BlittableJsonReaderObject);
            }
        }

        private void RegisterCompareExchangeValue(BlittableJsonReaderObject value)
        {
            if (_session.NoTracking || value == null)
                return;

            var v = CompareExchangeValueResultParser<BlittableJsonReaderObject>.GetSingleValue(value, _session.Conventions);
            if (v == null)
                return;

            _values[v.Key] = new CompareExchangeSessionValue
            {
                OriginalValue = v
            };
        }

        private bool TryGetCompareExchangeValueFromSession(string key, out CompareExchangeSessionValue value)
        {
            if (_values.TryGetValue(key, out value) == false)
                return false;

            if (value == null || value.OriginalValue.Index == -1)
                return false;

            return true;
        }

        private readonly Dictionary<string, CompareExchangeSessionValue> _values = new Dictionary<string, CompareExchangeSessionValue>(StringComparer.OrdinalIgnoreCase);

        private class CompareExchangeSessionValue
        {
            public CompareExchangeValue<BlittableJsonReaderObject> OriginalValue;

            private object _value;

            public CompareExchangeValue<T> GetValue<T>(DocumentConventions conventions)
            {
                if (_value is CompareExchangeValue<T> v)
                    return v;

                if (_value != null)
                    throw new InvalidOperationException("TODO ppekrol");

                var entity = (T)EntityToBlittable.ConvertToEntity(typeof(T), OriginalValue.Key, OriginalValue.Value, conventions);
                _value = new CompareExchangeValue<T>(OriginalValue.Key, OriginalValue.Index, entity);
                return _value;
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

        ILazyClusterTransactionOperations Lazily { get; }
    }

    public interface ILazyClusterTransactionOperations
    {
        Lazy<CompareExchangeValue<T>> GetCompareExchangeValue<T>(string key);

        Lazy<CompareExchangeValue<T>> GetCompareExchangeValue<T>(string key, Action<CompareExchangeValue<T>> onEval);

        Lazy<Dictionary<string, CompareExchangeValue<T>>> GetCompareExchangeValues<T>(string[] keys);

        Lazy<Dictionary<string, CompareExchangeValue<T>>> GetCompareExchangeValues<T>(string[] keys, Action<Dictionary<string, CompareExchangeValue<T>>> onEval);
    }

    public interface IClusterTransactionOperationsAsync : IClusterTransactionOperationsBase
    {
        Task<CompareExchangeValue<T>> GetCompareExchangeValueAsync<T>(string key, CancellationToken token = default);

        Task<Dictionary<string, CompareExchangeValue<T>>> GetCompareExchangeValuesAsync<T>(string[] keys, CancellationToken token = default);

        ILazyClusterTransactionOperationsAsync Lazily { get; }
    }

    public interface ILazyClusterTransactionOperationsAsync
    {
        Lazy<Task<CompareExchangeValue<T>>> GetCompareExchangeValueAsync<T>(string key, CancellationToken token = default);

        Lazy<Task<CompareExchangeValue<T>>> GetCompareExchangeValueAsync<T>(string key, Action<CompareExchangeValue<T>> onEval, CancellationToken token = default);

        Lazy<Task<Dictionary<string, CompareExchangeValue<T>>>> GetCompareExchangeValuesAsync<T>(string[] keys, CancellationToken token = default);

        Lazy<Task<Dictionary<string, CompareExchangeValue<T>>>> GetCompareExchangeValuesAsync<T>(string[] keys, Action<Dictionary<string, CompareExchangeValue<T>>> onEval, CancellationToken token = default);
    }

    public class ClusterTransactionOperationsAsync : ClusterTransactionOperationsBase<AsyncDocumentSession>, IClusterTransactionOperationsAsync, ILazyClusterTransactionOperationsAsync
    {
        public ClusterTransactionOperationsAsync(AsyncDocumentSession session) : base(session)
        {
        }

        ILazyClusterTransactionOperationsAsync IClusterTransactionOperationsAsync.Lazily => this;

        Lazy<Task<CompareExchangeValue<T>>> ILazyClusterTransactionOperationsAsync.GetCompareExchangeValueAsync<T>(string key, CancellationToken token)
        {
            return Session.AddLazyOperation<CompareExchangeValue<T>>(new LazyGetCompareExchangeValueOperation<T>(key, Session.Conventions, Session.Context), onEval: null, token);
        }

        Task<CompareExchangeValue<T>> IClusterTransactionOperationsAsync.GetCompareExchangeValueAsync<T>(string key, CancellationToken token)
        {
            return GetCompareExchangeValueAsyncInternal<T>(key, token);
        }

        Lazy<Task<CompareExchangeValue<T>>> ILazyClusterTransactionOperationsAsync.GetCompareExchangeValueAsync<T>(string key, Action<CompareExchangeValue<T>> onEval, CancellationToken token)
        {
            return Session.AddLazyOperation(new LazyGetCompareExchangeValueOperation<T>(key, Session.Conventions, Session.Context), onEval, token);
        }

        Lazy<Task<Dictionary<string, CompareExchangeValue<T>>>> ILazyClusterTransactionOperationsAsync.GetCompareExchangeValuesAsync<T>(string[] keys, CancellationToken token)
        {
            return Session.AddLazyOperation<Dictionary<string, CompareExchangeValue<T>>>(new LazyGetCompareExchangeValuesOperation<T>(keys, Session.Conventions, Session.Context), onEval: null, token);
        }

        Task<Dictionary<string, CompareExchangeValue<T>>> IClusterTransactionOperationsAsync.GetCompareExchangeValuesAsync<T>(string[] keys, CancellationToken token)
        {
            return GetCompareExchangeValuesAsyncInternal<T>(keys, token);
        }

        Lazy<Task<Dictionary<string, CompareExchangeValue<T>>>> ILazyClusterTransactionOperationsAsync.GetCompareExchangeValuesAsync<T>(string[] keys, Action<Dictionary<string, CompareExchangeValue<T>>> onEval, CancellationToken token)
        {
            return Session.AddLazyOperation(new LazyGetCompareExchangeValuesOperation<T>(keys, Session.Conventions, Session.Context), onEval, token);
        }
    }

    public class ClusterTransactionOperations : ClusterTransactionOperationsBase<DocumentSession>, IClusterTransactionOperations, ILazyClusterTransactionOperations
    {
        public ClusterTransactionOperations(DocumentSession session) : base(session)
        {
        }

        ILazyClusterTransactionOperations IClusterTransactionOperations.Lazily => this;

        CompareExchangeValue<T> IClusterTransactionOperations.GetCompareExchangeValue<T>(string key)
        {
            return AsyncHelpers.RunSync(() => GetCompareExchangeValueAsyncInternal<T>(key));
        }

        Lazy<CompareExchangeValue<T>> ILazyClusterTransactionOperations.GetCompareExchangeValue<T>(string key)
        {
            return Session.AddLazyOperation<CompareExchangeValue<T>>(new LazyGetCompareExchangeValueOperation<T>(key, Session.Conventions, Session.Context), onEval: null);
        }

        Lazy<CompareExchangeValue<T>> ILazyClusterTransactionOperations.GetCompareExchangeValue<T>(string key, Action<CompareExchangeValue<T>> onEval)
        {
            return Session.AddLazyOperation(new LazyGetCompareExchangeValueOperation<T>(key, Session.Conventions, Session.Context), onEval);
        }

        Dictionary<string, CompareExchangeValue<T>> IClusterTransactionOperations.GetCompareExchangeValues<T>(string[] keys)
        {
            return AsyncHelpers.RunSync(() => GetCompareExchangeValuesAsyncInternal<T>(keys));
        }

        Lazy<Dictionary<string, CompareExchangeValue<T>>> ILazyClusterTransactionOperations.GetCompareExchangeValues<T>(string[] keys)
        {
            return Session.AddLazyOperation<Dictionary<string, CompareExchangeValue<T>>>(new LazyGetCompareExchangeValuesOperation<T>(keys, Session.Conventions, Session.Context), onEval: null);
        }

        Lazy<Dictionary<string, CompareExchangeValue<T>>> ILazyClusterTransactionOperations.GetCompareExchangeValues<T>(string[] keys, Action<Dictionary<string, CompareExchangeValue<T>>> onEval)
        {
            return Session.AddLazyOperation(new LazyGetCompareExchangeValuesOperation<T>(keys, Session.Conventions, Session.Context), onEval);
        }
    }
}
