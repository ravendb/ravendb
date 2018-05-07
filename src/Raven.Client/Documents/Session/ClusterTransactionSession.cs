using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Util;

namespace Raven.Client.Documents.Session
{
    public abstract class ClusterSessionBase : AdvancedSessionExtentionBase
    {
        protected ClusterSessionBase(InMemoryDocumentSessionOperations session) : base(session)
        {
            if (session.TransactionMode != TransactionMode.ClusterWide)
            {
                throw new InvalidOperationException($"This function is part of cluster transaction session, in order to use it you have to open the Session with '{nameof(TransactionMode.ClusterWide)}' option.");
            }
        }

        public void CreateCompareExchangeValue<T>(string key, T item)
        {
            if (Session.StoreCompareExchange == null)
                Session.StoreCompareExchange = new Dictionary<string, (object Item, long Index)>();

            // new entity
            Session.StoreCompareExchange.Add(key, (item, 0));
        }

        public void DeleteCompareExchangeValue<T>(CompareExchangeValue<T> item)
        {
            if (Session.DeleteCompareExchange == null)
                Session.DeleteCompareExchange = new Dictionary<string, long>();

            if (Session.DeleteCompareExchange.ContainsKey(item.Key) == false)
                Session.DeleteCompareExchange.Add(item.Key, item.Index);
        }

        public void DeleteCompareExchangeValue(string key, long index)
        {
            if (Session.DeleteCompareExchange == null)
                Session.DeleteCompareExchange = new Dictionary<string, long>();

            if (Session.DeleteCompareExchange.ContainsKey(key) == false)
                Session.DeleteCompareExchange.Add(key, index);
        }

        public void UpdateCompareExchangeValue<T>(CompareExchangeValue<T> item)
        {
            Session.StoreCompareExchange[item.Key] = (item.Value, item.Index);
        }

        protected Task<CompareExchangeValue<T>> GetCompareExchangeValueAsyncInternal<T>(string key)
        {
            try
            {
                return Session.Operations.SendAsync(new GetCompareExchangeValueOperation<T>(key));
            }
            catch (Exception e)
            {
                throw new ClusterTransactionException($"Failed to get the compare exchange value for '{key}'", e);
            }
        }

        protected Task<Dictionary<string, long>> GetCompareExchangeIndexesInternal(string[] keys)
        {
            return Session.Operations.SendAsync(new GetCompareExchangeIndexOperation(keys));
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

        Task<Dictionary<string, long>> GetCompareExchangeIndexes(string[] keys);
    }

    public class ClusterTransactionOperationAsync : ClusterSessionBase, IClusterTransactionOperationAsync
    {
        public ClusterTransactionOperationAsync(InMemoryDocumentSessionOperations session) : base(session)
        {
        }

        public Task<CompareExchangeValue<T>> GetCompareExchangeValueAsync<T>(string key)
        {
            return GetCompareExchangeValueAsyncInternal<T>(key);
        }

        public Task<Dictionary<string, long>> GetCompareExchangeIndexes(string[] keys)
        {
            return GetCompareExchangeIndexesInternal(keys);
        }
    }

    public class ClusterTransactionOperation : ClusterSessionBase, IClusterTransactionOperation
    {
        public ClusterTransactionOperation(InMemoryDocumentSessionOperations session) : base(session)
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
