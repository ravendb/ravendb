using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session.Operations.Lazy;
using Raven.Client.Extensions;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
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
        internal readonly InMemoryDocumentSessionOperations _session;

        private readonly Dictionary<string, CompareExchangeSessionValue> _state = new Dictionary<string, CompareExchangeSessionValue>(StringComparer.OrdinalIgnoreCase);

        private Dictionary<string, string> _missingDocumentsToAtomicGuardIndex;

        internal bool TryGetMissingAtomicGuardFor(string docId, out string changeVector)
        {
            if (_missingDocumentsToAtomicGuardIndex == null)
            {
                changeVector = null;
                return false;
            }

            return _missingDocumentsToAtomicGuardIndex.TryGetValue(docId, out changeVector);
        }

        internal int NumberOfTrackedCompareExchangeValues => _state.Count;

        protected ClusterTransactionOperationsBase(InMemoryDocumentSessionOperations session)
        {
            if (session.TransactionMode != TransactionMode.ClusterWide)
            {
                throw new InvalidOperationException($"This function is part of cluster transaction session, in order to use it you have to open the Session with '{nameof(TransactionMode.ClusterWide)}' option.");
            }

            _session = session;
        }

        internal bool IsTracked(string key)
        {
            return TryGetCompareExchangeValueFromSession(key, out _);
        }

        public CompareExchangeValue<T> CreateCompareExchangeValue<T>(string key, T item)
        {
            if (key is null)
                throw new ArgumentNullException(nameof(key));

            if (TryGetCompareExchangeValueFromSession(key, out var sessionValue) == false)
                _state[key] = sessionValue = new CompareExchangeSessionValue(key, 0, CompareExchangeSessionValue.CompareExchangeValueState.None);

            return sessionValue.Create(item);
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
            var v = GetCompareExchangeValueFromSessionInternal<T>(key, out var notTracked);
            if (notTracked == false)
                return v;

            using (_session.AsyncTaskHolder())
            {
                _session.IncrementRequestCount();

                var value = await _session.Operations.SendAsync(new GetCompareExchangeValueOperation<BlittableJsonReaderObject>(key, materializeMetadata: false), sessionInfo: _session._sessionInfo, token: token).ConfigureAwait(false);
                if (value == null)
                {
                    RegisterMissingCompareExchangeValue(key);
                    return null;
                }

                var sessionValue = RegisterCompareExchangeValue(value);
                return sessionValue?.GetValue<T>(_session.Conventions);
            }
        }

        protected async Task<Dictionary<string, CompareExchangeValue<T>>> GetCompareExchangeValuesAsyncInternal<T>(string[] keys, CancellationToken token = default)
        {
            var results = GetCompareExchangeValuesFromSessionInternal<T>(keys, out var notTrackedKeys);

            if (notTrackedKeys == null || notTrackedKeys.Count == 0)
                return results;

            using (_session.AsyncTaskHolder())
            {
                _session.IncrementRequestCount();

                var keysArray = notTrackedKeys.ToArray();
                var values = await _session.Operations.SendAsync(new GetCompareExchangeValuesOperation<BlittableJsonReaderObject>(keysArray, materializeMetadata: false), sessionInfo: _session._sessionInfo, token: token).ConfigureAwait(false);

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

        protected async Task<Dictionary<string, CompareExchangeValue<T>>> GetCompareExchangeValuesAsyncInternal<T>(string startsWith, int start, int pageSize, CancellationToken token = default)
        {
            using (_session.AsyncTaskHolder())
            {
                _session.IncrementRequestCount();

                var values = await _session.Operations.SendAsync(new GetCompareExchangeValuesOperation<BlittableJsonReaderObject>(startsWith, start, pageSize), sessionInfo: _session.SessionInfo, token: token).ConfigureAwait(false);
                var results = new Dictionary<string, CompareExchangeValue<T>>();

                foreach (var keyValue in values)
                {
                    var key = keyValue.Key;
                    var value = keyValue.Value;

                    if (value == null)
                    {
                        RegisterMissingCompareExchangeValue(key);
                        results.Add(key, null);
                        continue;
                    }

                    var sessionValue = RegisterCompareExchangeValue(value);
                    results.Add(key, sessionValue.GetValue<T>(_session.Conventions));
                }

                return results;
            }
        }

        internal CompareExchangeValue<T> GetCompareExchangeValueFromSessionInternal<T>(string key, out bool notTracked)
        {
            if (TryGetCompareExchangeValueFromSession(key, out var sessionValue))
            {
                notTracked = false;
                return sessionValue.GetValue<T>(_session.Conventions);
            }

            notTracked = true;
            return null;
        }

        internal Dictionary<string, CompareExchangeValue<T>> GetCompareExchangeValuesFromSessionInternal<T>(string[] keys, out HashSet<string> notTrackedKeys)
        {
            notTrackedKeys = null;
            var results = new Dictionary<string, CompareExchangeValue<T>>(StringComparer.OrdinalIgnoreCase);
            if (keys == null || keys.Length == 0)
                return results;

            foreach (var key in keys)
            {
                if (TryGetCompareExchangeValueFromSession(key, out var sessionValue))
                {
                    results[key] = sessionValue.GetValue<T>(_session.Conventions);
                    continue;
                }

                if (notTrackedKeys == null)
                    notTrackedKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                notTrackedKeys.Add(key);
            }

            return results;
        }

        internal CompareExchangeSessionValue RegisterMissingCompareExchangeValue(string key)
        {
            var value = new CompareExchangeSessionValue(key, -1, CompareExchangeSessionValue.CompareExchangeValueState.Missing);
            if (_session.NoTracking)
                return value;

            _state.Add(key, value);
            return value;
        }

        internal void RegisterCompareExchangeValues(BlittableJsonReaderObject values, bool includingMissingAtomicGuards)
        {
            if (_session.NoTracking)
                return;

            if (values != null)
            {
                var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
                for (var i = 0; i < values.Count; i++)
                {
                    values.GetPropertyByIndex(i, ref propertyDetails);

                    var value = propertyDetails.Value as BlittableJsonReaderObject;

                    var val = CompareExchangeValueResultParser<BlittableJsonReaderObject>.GetSingleValue(value, materializeMetadata: false, _session.Conventions);
                    if(includingMissingAtomicGuards  &&
                        val.Key.StartsWith(Constants.CompareExchange.RvnAtomicPrefix, StringComparison.OrdinalIgnoreCase) && 
                        val.ChangeVector != null)
                    {
                        _missingDocumentsToAtomicGuardIndex ??= new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                        _missingDocumentsToAtomicGuardIndex.Add(val.Key.Substring(Constants.CompareExchange.RvnAtomicPrefix.Length), val.ChangeVector);
                    }
                    else
                    {
                        RegisterCompareExchangeValue(val);
                    }
                }
            }
        }

        internal CompareExchangeSessionValue RegisterCompareExchangeValue(CompareExchangeValue<BlittableJsonReaderObject> value)
        {
            Debug.Assert(value != null, "value != null");

            if (value.Key.StartsWith(Constants.CompareExchange.RvnAtomicPrefix, StringComparison.InvariantCultureIgnoreCase))
                throw new InvalidOperationException($"'{value.Key}' is an atomic guard and you cannot load it via the session");

            if (_session.NoTracking)
                return new CompareExchangeSessionValue(value);

            if (_state.TryGetValue(value.Key, out var sessionValue) == false)
                return _state[value.Key] = new CompareExchangeSessionValue(value);

            sessionValue.UpdateValue(value, _session);

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

        internal class CompareExchangeSessionValue
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
                                throw new InvalidOperationException("Value cannot be null.");

                            T entity = default;
                            if (_originalValue != null && _originalValue.Value != null)
                            {
                                var type = typeof(T);
                                if (type.IsPrimitive || type == typeof(string))
                                    _originalValue.Value.TryGet(Constants.CompareExchange.ObjectFieldName, out entity);
                                else
                                    entity = conventions.Serialization.DefaultConverter.FromBlittable<T>(_originalValue.Value, _key);
                            }

                            var value = new CompareExchangeValue<T>(_key, _index, entity, _originalValue?.Metadata);
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

            internal CompareExchangeValue<T> Create<T>(T item)
            {
                AssertState();

                if (_value != null)
                    throw new InvalidOperationException($"The compare exchange value with key '{_key}' is already tracked.");

                _index = 0;
                var value = new CompareExchangeValue<T>(_key, _index, item);
                _value = value;
                _state = CompareExchangeValueState.Created;
                return value;
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

            internal ICommandData GetCommand(DocumentConventions conventions, JsonOperationContext context, IJsonSerializer jsonSerializer)
            {
                switch (_state)
                {
                    case CompareExchangeValueState.None:
                    case CompareExchangeValueState.Created:
                        if (_value == null)
                            return null;

                        var entity = CompareExchangeValueBlittableJsonConverter.ConvertToBlittable(_value.Value, conventions, context, jsonSerializer);
                        var entityJson = entity as BlittableJsonReaderObject;
                        BlittableJsonReaderObject metadata = null;
                        _originalValue?.Value?.TryGet(Constants.Documents.Metadata.Key, out metadata);
                        var metadataHasChanged = false;
                        if (_value.HasMetadata && _value.Metadata.Count != 0)
                        {
                            if (metadata == null)
                            {
                                metadataHasChanged = true;
                                metadata = PrepareMetadataForPut(_key, _value.Metadata, conventions, context); //create new metadata (because there wasn't any metadata before)
                            }
                            else
                            {
                                ValidateMetadataForPut(_key, _value.Metadata);
                                metadataHasChanged = InMemoryDocumentSessionOperations.UpdateMetadataModifications(_value.Metadata, metadata); //add modifications to the existing metadata
                            }
                        }

                        BlittableJsonReaderObject entityToInsert = null;

                        if (entityJson == null || metadataHasChanged)
                            entityJson = entityToInsert = ConvertEntity(_key, entity, metadata);

                        var newValue = new CompareExchangeValue<BlittableJsonReaderObject>(_key, _index, entityJson);

                        var hasChanged = _originalValue == null || metadataHasChanged || HasChanged(_originalValue, newValue);
                        _originalValue = newValue;

                        if (hasChanged == false)
                            return null;

                        if (entityToInsert == null)
                            entityToInsert = ConvertEntity(_key, entity, metadata);

                        return new PutCompareExchangeCommandData(newValue.Key, entityToInsert, newValue.Index);
                    case CompareExchangeValueState.Deleted:
                        return new DeleteCompareExchangeCommandData(_key, _index);
                    case CompareExchangeValueState.Missing:
                        return null;
                    default:
                        throw new NotSupportedException($"Not supported state: '{_state}'");
                }

                BlittableJsonReaderObject ConvertEntity(string key, object entity, BlittableJsonReaderObject metadata = null)
                {
                    var djv = new DynamicJsonValue
                    {
                        [Constants.CompareExchange.ObjectFieldName] = entity
                    };

                    if (metadata == null)
                        return context.ReadObject(djv, key);

                    djv[Constants.Documents.Metadata.Key] = metadata;
                    return context.ReadObject(djv, key);
                }
            }

            internal bool HasChanged(CompareExchangeValue<BlittableJsonReaderObject> originalValue, CompareExchangeValue<BlittableJsonReaderObject> newValue)
            {
                if (ReferenceEquals(originalValue, newValue))
                    return false;

                if (string.Equals(originalValue.Key, newValue.Key, StringComparison.OrdinalIgnoreCase) == false)
                    throw new InvalidOperationException($"Keys do not match. Expected '{originalValue.Key}' but was '{newValue.Key}'.");

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

            internal void UpdateValue(CompareExchangeValue<BlittableJsonReaderObject> value, InMemoryDocumentSessionOperations session)
            {
                _index = value.Index;
                _state = value.Index >= 0 ? CompareExchangeValueState.None : CompareExchangeValueState.Missing;

                _originalValue = value;

                if (_value != null)
                {
                    _value.Index = _index;

                    if (_value.Value != null)
                        session.JsonConverter.PopulateEntity(_value.Value, value.Value, session.JsonSerializer);
                }
            }

            internal static BlittableJsonReaderObject PrepareMetadataForPut(string key, IMetadataDictionary metadataDictionary, DocumentConventions conventions, JsonOperationContext context)
            {
                ValidateMetadataForPut(key, metadataDictionary);

                using (var writer = conventions.Serialization.CreateWriter(context))
                {
                    writer.WriteMetadata(metadataDictionary);
                    writer.FinalizeDocument();
                    return writer.CreateReader();
                }
            }

            private static void ValidateMetadataForPut(string key, IMetadataDictionary metadataDictionary)
            {
                if (metadataDictionary.TryGetValue(Constants.Documents.Metadata.Expires, out object obj))
                {
                    if (obj == null)
                        ThrowInvalidExpiresMetadata($"The value of {Constants.Documents.Metadata.Expires} metadata for compare exchange '{key}' is null.");
                    if (obj is DateTime == false && obj is string == false)
                        ThrowInvalidExpiresMetadata($"The type of {Constants.Documents.Metadata.Expires} metadata for compare exchange '{key}' is not valid. Use the following type: {nameof(DateTime)} or {nameof(String)}");
                }
            }

            private static void ThrowInvalidExpiresMetadata(string message)
            {
                throw new ArgumentException(message);
            }
        }
    }

    public interface IClusterTransactionOperationsBase
    {
        void DeleteCompareExchangeValue(string key, long index);

        void DeleteCompareExchangeValue<T>(CompareExchangeValue<T> item);

        CompareExchangeValue<T> CreateCompareExchangeValue<T>(string key, T value);
    }

    public interface IClusterTransactionOperations : IClusterTransactionOperationsBase
    {
        CompareExchangeValue<T> GetCompareExchangeValue<T>(string key);

        Dictionary<string, CompareExchangeValue<T>> GetCompareExchangeValues<T>(string[] keys);

        Dictionary<string, CompareExchangeValue<T>> GetCompareExchangeValues<T>(string startsWith, int start = 0, int pageSize = 25);

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

        Task<Dictionary<string, CompareExchangeValue<T>>> GetCompareExchangeValuesAsync<T>(string startsWith, int start = 0, int pageSize = 25, CancellationToken token = default);

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
            return Session.AddLazyOperation<CompareExchangeValue<T>>(new LazyGetCompareExchangeValueOperation<T>(this, Session.Conventions, key), onEval: null, token);
        }

        Task<CompareExchangeValue<T>> IClusterTransactionOperationsAsync.GetCompareExchangeValueAsync<T>(string key, CancellationToken token)
        {
            return GetCompareExchangeValueAsyncInternal<T>(key, token);
        }

        Task<Dictionary<string, CompareExchangeValue<T>>> IClusterTransactionOperationsAsync.GetCompareExchangeValuesAsync<T>(string startsWith, int start, int pageSize, CancellationToken token)
        {
            return GetCompareExchangeValuesAsyncInternal<T>(startsWith, start, pageSize, token);
        }

        Lazy<Task<CompareExchangeValue<T>>> ILazyClusterTransactionOperationsAsync.GetCompareExchangeValueAsync<T>(string key, Action<CompareExchangeValue<T>> onEval, CancellationToken token)
        {
            return Session.AddLazyOperation(new LazyGetCompareExchangeValueOperation<T>(this, Session.Conventions, key), onEval, token);
        }

        Lazy<Task<Dictionary<string, CompareExchangeValue<T>>>> ILazyClusterTransactionOperationsAsync.GetCompareExchangeValuesAsync<T>(string[] keys, CancellationToken token)
        {
            return Session.AddLazyOperation<Dictionary<string, CompareExchangeValue<T>>>(new LazyGetCompareExchangeValuesOperation<T>(this, Session.Conventions, keys), onEval: null, token);
        }

        Task<Dictionary<string, CompareExchangeValue<T>>> IClusterTransactionOperationsAsync.GetCompareExchangeValuesAsync<T>(string[] keys, CancellationToken token)
        {
            return GetCompareExchangeValuesAsyncInternal<T>(keys, token);
        }

        Lazy<Task<Dictionary<string, CompareExchangeValue<T>>>> ILazyClusterTransactionOperationsAsync.GetCompareExchangeValuesAsync<T>(string[] keys, Action<Dictionary<string, CompareExchangeValue<T>>> onEval, CancellationToken token)
        {
            return Session.AddLazyOperation(new LazyGetCompareExchangeValuesOperation<T>(this, Session.Conventions, keys), onEval, token);
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
            return Session.AddLazyOperation<CompareExchangeValue<T>>(new LazyGetCompareExchangeValueOperation<T>(this, Session.Conventions, key), onEval: null);
        }

        Lazy<CompareExchangeValue<T>> ILazyClusterTransactionOperations.GetCompareExchangeValue<T>(string key, Action<CompareExchangeValue<T>> onEval)
        {
            return Session.AddLazyOperation(new LazyGetCompareExchangeValueOperation<T>(this, Session.Conventions, key), onEval);
        }

        Dictionary<string, CompareExchangeValue<T>> IClusterTransactionOperations.GetCompareExchangeValues<T>(string[] keys)
        {
            return AsyncHelpers.RunSync(() => GetCompareExchangeValuesAsyncInternal<T>(keys));
        }

        Lazy<Dictionary<string, CompareExchangeValue<T>>> ILazyClusterTransactionOperations.GetCompareExchangeValues<T>(string[] keys)
        {
            return Session.AddLazyOperation<Dictionary<string, CompareExchangeValue<T>>>(new LazyGetCompareExchangeValuesOperation<T>(this, Session.Conventions, keys), onEval: null);
        }

        Dictionary<string, CompareExchangeValue<T>> IClusterTransactionOperations.GetCompareExchangeValues<T>(string startsWith, int start, int pageSize)
        {
            return AsyncHelpers.RunSync(() => GetCompareExchangeValuesAsyncInternal<T>(startsWith, start, pageSize));
        }

        Lazy<Dictionary<string, CompareExchangeValue<T>>> ILazyClusterTransactionOperations.GetCompareExchangeValues<T>(string[] keys, Action<Dictionary<string, CompareExchangeValue<T>>> onEval)
        {
            return Session.AddLazyOperation(new LazyGetCompareExchangeValuesOperation<T>(this, Session.Conventions, keys), onEval);
        }
    }
}
