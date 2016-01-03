// -----------------------------------------------------------------------
//  <copyright file="SubscriptionChannel.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client.Extensions;
using Raven.Json.Linq;

namespace Raven.Client.Document
{
    public class DocumentSubscriptions : IReliableSubscriptions
    {
        private readonly AsyncDocumentSubscriptions innerAsync;

        public DocumentSubscriptions(IDocumentStore documentStore)
        {
            innerAsync = new AsyncDocumentSubscriptions(documentStore);
        }

        public long Create(SubscriptionCriteria criteria, string database = null)
        {
            return AsyncHelpers.RunSync(() => innerAsync.CreateAsync(criteria, database));
        }

        public long Create<T>(SubscriptionCriteria<T> criteria, string database = null)
        {
            return AsyncHelpers.RunSync(() =>  innerAsync.CreateAsync(criteria, database));
        }

        public Subscription<RavenJObject> Open(long id, SubscriptionConnectionOptions options, string database = null)
        {
            return AsyncHelpers.RunSync(() =>  innerAsync.OpenAsync(id, options, database));
        }

        public Subscription<T> Open<T>(long id, SubscriptionConnectionOptions options, string database = null) where T : class 
        {
            return AsyncHelpers.RunSync(() =>  innerAsync.OpenAsync<T>(id, options, database));
        }

        public List<SubscriptionConfig> GetSubscriptions(int start, int take, string database = null)
        {
            return AsyncHelpers.RunSync(() =>  innerAsync.GetSubscriptionsAsync(start, take, database));
        }

        public void Delete(long id, string database = null)
        {
            AsyncHelpers.RunSync(() =>  innerAsync.DeleteAsync(id, database));
        }

        public void Release(long id, string database = null)
        {
            AsyncHelpers.RunSync(() =>  innerAsync.ReleaseAsync(id, database));
        }

        public void Dispose()
        {
            innerAsync.Dispose();
        }
    }
}
