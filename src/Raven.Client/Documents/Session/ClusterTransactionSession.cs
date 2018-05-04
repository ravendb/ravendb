using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Util;

namespace Raven.Client.Documents.Session
{
    public abstract class ClusterSessionBase : AdvancedSessionExtentionBase
    {
        protected Dictionary<string, long> TrackedKeys;
        protected bool Marked;

        protected ClusterSessionBase(InMemoryDocumentSessionOperations session) : base(session)
        {
        }

        protected void CreateCompareExchangeValueInternal<T>(string key, T item)
        {
            EnsureClusterSession();

            if (Session.StoreCompareExchange == null)
                Session.StoreCompareExchange = new Dictionary<string, (object Item, long Index)>();

            // new entity
            Session.StoreCompareExchange.Add(key, (item, 0));
        }

        protected async Task DeleteCompareExchangeValueAsyncInternal(string key, long? index = null)
        {
            EnsureClusterSession();

            if (Session.DeleteCompareExchange == null)
                Session.DeleteCompareExchange = new Dictionary<string, long>();

            if (TrackedKeys == null)
                TrackedKeys = new Dictionary<string, long>();

            if (index != null)
            {
                if (Session.DeleteCompareExchange.ContainsKey(key) == false)
                    Session.DeleteCompareExchange.Add(key, index.Value);
                return;
            }

            if (TrackedKeys.TryGetValue(key, out var deletionIndex))
            {
                Session.DeleteCompareExchange[key] = deletionIndex;
                return;
            }

            var result = await Session.Operations.SendAsync(new GetCompareExchangeIndexOperation(key)).ConfigureAwait(false);
            Session.DeleteCompareExchange[key] = result.Indexes[0];
        }

        protected void UpdateCompareExchangeValueInternal<T>(CompareExchangeValue<T> item)
        {
            EnsureClusterSession();

            Session.StoreCompareExchange[item.Key] = (item.Value, item.Index);
        }

        protected async Task<CompareExchangeValue<T>> GetCompareExchangeValueAsyncInternal<T>(string key)
        {
            EnsureClusterSession();

            try
            {
                return await Session.Operations.SendAsync(new GetCompareExchangeValueOperation<T>(key)).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new ClusterTransactionException($"Failed to get the compare exchange value for '{key}'", e);
            }
        }

        private void EnsureClusterSession()
        {
            if (Marked)
                return;

            if (Session.TransactionMode != TransactionMode.ClusterWide)
            {
                throw new InvalidOperationException($"This function is part of cluster transaction session, in order to use it you have to create the Session with '{nameof(TransactionMode.ClusterWide)}'.");
            }
            Marked = true;
        }
    }

    public interface IClusterTransactionOperation
    {
        void DeleteCompareExchangeValue(string key, long? index = null);

        CompareExchangeValue<T> GetCompareExchangeValue<T>(string key);

        void UpdateCompareExchangeValue<T>(CompareExchangeValue<T> item);

        void CreateCompareExchangeValue<T>(string key, T value);
    }

    public interface IClusterTransactionOperationAsync
    {
        Task DeleteCompareExchangeValueAsync(string key, long? index = null);

        Task<CompareExchangeValue<T>> GetCompareExchangeValueAsync<T>(string key);

        void UpdateCompareExchangeValue<T>(CompareExchangeValue<T> item);

        void CreateCompareExchangeValue<T>(string key, T value);
    }

    public class ClusterTransactionOperationAsync : ClusterSessionBase, IClusterTransactionOperationAsync
    {
        public ClusterTransactionOperationAsync(InMemoryDocumentSessionOperations session) : base(session)
        {
        }

        public async Task DeleteCompareExchangeValueAsync(string key, long? index = null)
        {
            await DeleteCompareExchangeValueAsyncInternal(key, index).ConfigureAwait(false);
        }

        public async Task<CompareExchangeValue<T>> GetCompareExchangeValueAsync<T>(string key)
        {
            return await GetCompareExchangeValueAsyncInternal<T>(key).ConfigureAwait(false);
        }

        public void UpdateCompareExchangeValue<T>(CompareExchangeValue<T> item)
        {
            UpdateCompareExchangeValueInternal(item);
        }

        public void CreateCompareExchangeValue<T>(string key, T item)
        {
            CreateCompareExchangeValueInternal(key, item);
        }
    }

    public class ClusterTransactionOperation : ClusterSessionBase, IClusterTransactionOperation
    {
        public ClusterTransactionOperation(InMemoryDocumentSessionOperations session) : base(session)
        {
        }

        public void DeleteCompareExchangeValue(string key, long? index = null)
        {
            AsyncHelpers.RunSync(() => DeleteCompareExchangeValueAsyncInternal(key, index));
        }

        public CompareExchangeValue<T> GetCompareExchangeValue<T>(string key)
        {
            return AsyncHelpers.RunSync(() => GetCompareExchangeValueAsyncInternal<T>(key));
        }

        public void UpdateCompareExchangeValue<T>(CompareExchangeValue<T> item)
        {
            UpdateCompareExchangeValueInternal(item);
        }

        public void CreateCompareExchangeValue<T>(string key, T item)
        {
            CreateCompareExchangeValueInternal(key, item);
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
