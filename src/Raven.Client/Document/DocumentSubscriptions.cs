// -----------------------------------------------------------------------
//  <copyright file="SubscriptionChannel.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using Raven.Client.Data;
using Raven.Client.Util;

namespace Raven.Client.Document
{
    public class DocumentSubscriptions : IReliableSubscriptions
    {
        private readonly AsyncDocumentSubscriptions _innerAsync;

        public DocumentSubscriptions(IDocumentStore documentStore)
        {
            _innerAsync = new AsyncDocumentSubscriptions(documentStore);
        }

        public long Create(SubscriptionCriteria criteria, long startEtag = 0, string database = null)
        {
            return AsyncHelpers.RunSync(() => _innerAsync.CreateAsync(criteria, startEtag, database));
        }

        public long Create<T>(SubscriptionCriteria<T> criteria, long startEtag = 0, string database = null)
        {
            return AsyncHelpers.RunSync(() => _innerAsync.CreateAsync(criteria, startEtag, database));
        }

        public Subscription<dynamic> Open(SubscriptionConnectionOptions options, string database = null)
        {
            return _innerAsync.Open<dynamic>(options, database);
        }

        public Subscription<T> Open<T>(SubscriptionConnectionOptions options, string database = null) where T : class
        {
            return _innerAsync.Open<T>(options, database);
        }

        public List<SubscriptionConfig> GetSubscriptions(int start, int take, string database = null)
        {
            return AsyncHelpers.RunSync(() => _innerAsync.GetSubscriptionsAsync(start, take, database));
        }

        public void Delete(long id, string database = null)
        {
            AsyncHelpers.RunSync(() => _innerAsync.DeleteAsync(id, database));
        }

        public void Dispose()
        {
            _innerAsync.Dispose();
        }
    }
}