using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session.Operations.Lazy;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;

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

        private readonly Dictionary<string, CompareExchangeSessionValue> _state = new Dictionary<string, CompareExchangeSessionValue>(StringComparer.OrdinalIgnoreCase);

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
            if (key is null)
                throw new ArgumentNullException(nameof(key));

            if (TryGetCompareExchangeValueFromSession(key, out var sessionValue) == false)
                _state[key] = sessionValue = new CompareExchangeSessionValue(key, 0, CompareExchangeSessionValue.CompareExchangeValueState.None);

            sessionValue.Create(item);
        }

        public void DeleteCompareExchangeValue<T>(CompareExchangeValue<T> item)
        {
            if (item is null)
                throw new ArgumentNullException(nameof(item));

            if (TryGetCompareExchangeValueFromSession(item.Key, out var sessionValue) == false)
                _state[item.Key] = sessionValue = new CompareExchangeSessionValue(item.Key, 0, CompareExchangeSessionValue.CompareExchangeValueState.None);

            sessionValue.Delete(item.Index);
        }

        public void DeleteCompareExchangeValue(string key, long index)
        {
            if (key is null)
                throw new ArgumentNullException(nameof(key));

            if (TryGetCompareExchangeValueFromSession(key, out var sessionValue) == false)
                _state[key] = sessionValue = new CompareExchangeSessionValue(key, 0, CompareExchangeSessionValue.CompareExchangeValueState.None);

            sessionValue.Delete(index);
        }

        public void Clear()
        {
            _state.Clear();
        }

        protected async Task<CompareExchangeValue<T>> GetCompareExchangeValueAsyncInternal<T>(string key, CancellationToken token = default)
        {
            if (TryGetCompareExchangeValueFromSession(key, out var sessionValue))
                return sessionValue.GetValue<T>(_session.Conventions);

            using (_session.AsyncTaskHolder())
            {
                _session.IncrementRequestCount();

                var value = await _session.Operations.SendAsync(new GetCompareExchangeValueOperation<BlittableJsonReaderObject>(key), sessionInfo: _session.SessionInfo, token: token).ConfigureAwait(false);
                if (value == null)
                {
                    RegisterMissingCompareExchangeValue(key);
                    return null;
                }

                sessionValue = RegisterCompareExchangeValue(value);
                return sessionValue?.GetValue<T>(_session.Conventions);
            }
        }

        protected async Task<Dictionary<string, CompareExchangeValue<T>>> GetCompareExchangeValuesAsyncInternal<T>(string[] keys, CancellationToken token = default)
        {
            var results = new Dictionary<string, CompareExchangeValue<T>>(StringComparer.OrdinalIgnoreCase);
            if (keys == null || keys.Length == 0)
                return results;

            HashSet<string> missingKeys = null;
            foreach (var key in keys)
            {
                if (TryGetCompareExchangeValueFromSession(key, out var sessionValue))
                {
                    results[key] = sessionValue.GetValue<T>(_session.Conventions);
                    continue;
                }

                if (missingKeys == null)
                    missingKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                missingKeys.Add(key);
            }

            if (missingKeys == null || missingKeys.Count == 0)
                return results;

            using (_session.AsyncTaskHolder())
            {
                _session.IncrementRequestCount();

                var keysArray = missingKeys.ToArray();
                var values = await _session.Operations.SendAsync(new GetCompareExchangeValuesOperation<BlittableJsonReaderObject>(keysArray), sessionInfo: _session.SessionInfo, token: token).ConfigureAwait(false);

                foreach (var key in keysArray)
                {
                    if (values.TryGetValue(key, out var value) == false || value == null)
                    {
                        RegisterMissingCompareExchangeValue(key);
                        results.Add(key, null);
                        continue;
                    }

                    var sessionValue = RegisterCompareExchangeValue(value);
                    results.Add(value.Key, sessionValue.GetValue<T>(_session.Conventions));
                }

                return results;
            }
        }

        private void RegisterMissingCompareExchangeValue(string key)
        {
            if (_session.NoTracking)
                return;

            _state.Add(key, new CompareExchangeSessionValue(key, -1, CompareExchangeSessionValue.CompareExchangeValueState.Missing));
        }

        internal void RegisterCompareExchangeValues(BlittableJsonReaderObject values)
        {
            if (_session.NoTracking || values == null)
                return;

            var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
            for (var i = 0; i < values.Count; i++)
            {
                values.GetPropertyByIndex(i, ref propertyDetails);

                var value = propertyDetails.Value as BlittableJsonReaderObject;

                RegisterCompareExchangeValue(CompareExchangeValueResultParser<BlittableJsonReaderObject>.GetSingleValue(value, _session.Conventions));
            }
        }

        private CompareExchangeSessionValue RegisterCompareExchangeValue(CompareExchangeValue<BlittableJsonReaderObject> value)
        {
            Debug.Assert(value != null, "value != null");

            if (_state.TryGetValue(value.Key, out var sessionValue) == false)
                return _state[value.Key] = new CompareExchangeSessionValue(value);

            sessionValue.UpdateValue(value, _session.JsonSerializer);

            return sessionValue;
        }

        private bool TryGetCompareExchangeValueFromSession(string key, out CompareExchangeSessionValue value)
        {
            if (_state.TryGetValue(key, out value) == false || value == null)
                return false;

            return true;
        }

        internal void PrepareCompareExchangeEntities(InMemoryDocumentSessionOperations.SaveChangesData result)
        {
            if (_state.Count == 0)
                return;

            foreach (var kvp in _state)
            {
                var command = kvp.Value.GetCommand(_session.Conventions, _session.Context, _session.JsonSerializer);
                if (command == null)
                    continue;

                result.SessionCommands.Add(command);
            }
        }

        internal void UpdateState(string key, long index)
        {
            if (TryGetCompareExchangeValueFromSession(key, out var sessionValue) == false)
                return;

            sessionValue.UpdateState(index);
        }

        private class CompareExchangeSessionValue
        {
            private readonly string _key;

            private long _index;

            private CompareExchangeValue<BlittableJsonReaderObject> _originalValue;

            private ICompareExchangeValue _value;

            public CompareExchangeValueState _state;

            public CompareExchangeSessionValue(string key, long index, CompareExchangeValueState state)
            {
                _key = key ?? throw new ArgumentNullException(nameof(key));
                _index = index;
                _state = state;
            }

            public CompareExchangeSessionValue(CompareExchangeValue<BlittableJsonReaderObject> value)
                : this(value.Key, value.Index, value.Index >= 0 ? CompareExchangeValueState.None : CompareExchangeValueState.Missing)
            {
                if (value.Index > 0)
                    _originalValue = value;
            }

            internal CompareExchangeValue<T> GetValue<T>(DocumentConventions conventions)
            {
                switch (_state)
                {
                    case CompareExchangeValueState.None:
                    case CompareExchangeValueState.Created:
                        {
                            if (_value is CompareExchangeValue<T> v)
                                return v;

                            if (_value != null)
                                throw new InvalidOperationException("TODO ppekrol");

                            T entity = default;
                            if (_originalValue != null && _originalValue.Value != null)
                                entity = (T)EntityToBlittable.ConvertToEntity(typeof(T), _key, _originalValue.Value, conventions);

                            var value = new CompareExchangeValue<T>(_key, _index, entity);
                            _value = value;

                            return value;
                        }
                    case CompareExchangeValueState.Missing:
                    case CompareExchangeValueState.Deleted:
                        return null;
                    default:
                        throw new NotSupportedException($"Not supported state: '{_state}'");
                }
            }

            internal void Create<T>(T item)
            {
                AssertState();

                if (_value != null)
                    throw new InvalidOperationException($"The compare exchange value with key '{_key}' is already tracked.");

                _index = 0;
                _value = new CompareExchangeValue<T>(_key, _index, item);
                _state = CompareExchangeValueState.Created;
            }

            internal void Delete(long index)
            {
                AssertState();

                _index = index;
                _state = CompareExchangeValueState.Deleted;
            }

            private void AssertState()
            {
                switch (_state)
                {
                    case CompareExchangeValueState.None:
                    case CompareExchangeValueState.Missing:
                        return;
                    case CompareExchangeValueState.Created:
                        throw new InvalidOperationException($"The compare exchange value with key '{_key}' was already stored.");
                    case CompareExchangeValueState.Deleted:
                        throw new InvalidOperationException($"The compare exchange value with key '{_key}' was already deleted.");
                }
            }

            internal ICommandData GetCommand(DocumentConventions conventions, JsonOperationContext context, JsonSerializer jsonSerializer)
            {
                switch (_state)
                {
                    case CompareExchangeValueState.None:
                    case CompareExchangeValueState.Created:
                        if (_value == null)
                            return null;

                        var entity = EntityToBlittable.ConvertToBlittableForCompareExchangeIfNeeded(_value.Value, conventions, context, jsonSerializer, documentInfo: null, removeIdentityProperty: false);
                        var entityJson = entity as BlittableJsonReaderObject;
                        BlittableJsonReaderObject entityToInsert = null;
                        if (entityJson == null)
                            entityJson = entityToInsert = ConvertEntity(_key, entity);

                        var newValue = new CompareExchangeValue<BlittableJsonReaderObject>(_key, _index, entityJson);

                        var hasChanged = _originalValue == null || HasChanged(_originalValue, newValue);
                        _originalValue = newValue;

                        if (hasChanged == false)
                            return null;

                        if (entityToInsert == null)
                            entityToInsert = ConvertEntity(_key, entity);

                        return new PutCompareExchangeCommandData(newValue.Key, entityToInsert, newValue.Index);
                    case CompareExchangeValueState.Deleted:
                        return new DeleteCompareExchangeCommandData(_key, _index);
                    case CompareExchangeValueState.Missing:
                        return null;
                    default:
                        throw new NotSupportedException($"Not supprted state: '{_state}'");
                }

                BlittableJsonReaderObject ConvertEntity(string key, object entity)
                {
                    var djv = new DynamicJsonValue
                    {
                        [Constants.CompareExchange.ObjectFieldName] = entity
                    };
                    return context.ReadObject(djv, key);
                }
            }

            internal bool HasChanged(CompareExchangeValue<BlittableJsonReaderObject> originalValue, CompareExchangeValue<BlittableJsonReaderObject> newValue)
            {
                if (ReferenceEquals(originalValue, newValue))
                    return false;

                if (string.Equals(originalValue.Key, newValue.Key, StringComparison.OrdinalIgnoreCase) == false)
                    throw new InvalidOperationException("TODO ppekrol");

                if (originalValue.Index != newValue.Index)
                    return true;

                if (originalValue.Value == null)
                    return true;

                return originalValue.Value.Equals(newValue.Value) == false;
            }

            public enum CompareExchangeValueState
            {
                None,
                Created,
                Deleted,
                Missing
            }

            internal void UpdateState(long index)
            {
                _index = index;
                _state = CompareExchangeValueState.None;

                if (_originalValue != null)
                    _originalValue.Index = index;

                if (_value != null)
                    _value.Index = index;
            }

            internal void UpdateValue(CompareExchangeValue<BlittableJsonReaderObject> value, JsonSerializer jsonSerializer)
            {
                _index = value.Index;
                _state = value.Index >= 0 ? CompareExchangeValueState.None : CompareExchangeValueState.Missing;

                _originalValue = value;

                if (_value != null)
                {
                    _value.Index = _index;

                    if (_value.Value != null)
                        EntityToBlittable.PopulateEntity(_value.Value, value.Value, jsonSerializer);
                }
            }
        }
    }

    public interface IClusterTransactionOperationsBase
    {
        void DeleteCompareExchangeValue(string key, long index);

        void DeleteCompareExchangeValue<T>(CompareExchangeValue<T> item);

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
