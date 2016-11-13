using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Client.Document
{
    public interface IReliableSubscriptions : IDisposable
    {
        /// <summary>
        /// It creates a data subscription in a database. The subscription will expose all documents that match the specified subscription criteria for a given type.
        /// </summary>
        /// <returns>Created subscription identifier.</returns>
        long Create<T>(SubscriptionCriteria<T> criteria,long startEtag = 0,  string database = null);

        /// <summary>
        /// It creates a data subscription in a database. The subscription will expose all documents that match the specified subscription criteria.
        /// </summary>
        /// <returns>Created subscription identifier.</returns>
        long Create(SubscriptionCriteria criteria, long startEtag = 0, string database = null);

        /// <summary>
        /// It opens a subscription and starts pulling documents since a last processed document for that subscription (in document's long? order).
        /// The connection options determine client and server cooperation rules like document batch sizes or a timeout in a matter of which a client
        /// needs to acknowledge that batch has been processed. The acknowledgment is sent after all documents are processed by subscription's handlers.  
        /// There can be only a single client that is connected to a subscription.
        /// </summary>
        /// <returns>Subscription object that allows to add/remove subscription handlers.</returns>
        Subscription<RavenJObject> Open(SubscriptionConnectionOptions options, string database = null);

        /// <summary>
        /// It opens a subscription and starts pulling documents since a last processed document for that subscription (in document's long? order).
        /// The connection options determine client and server cooperation rules like document batch sizes or a timeout in a matter of which a client
        /// needs to acknowledge that batch has been processed. The acknowledgment is sent after all documents are processed by subscription's handlers.  
        /// There can be only a single client that is connected to a subscription.
        /// </summary>
        /// <returns>Subscription object that allows to add/remove subscription handlers.</returns>
        Subscription<T> Open<T>(SubscriptionConnectionOptions options, string database = null) where T : class;

        /// <summary>
        /// It downloads a list of all existing subscriptions in a database.
        /// </summary>
        /// <returns>Existing subscriptions' configurations.</returns>
        List<SubscriptionConfig> GetSubscriptions(int start, int take, string database = null);

        /// <summary>
        /// It deletes a subscription.
        /// </summary>
        void Delete(long id, string database = null);

        /// <summary>
        /// It releases a subscriptions by forcing a connected client to drop.
        /// </summary>
        void Release(long id, string database = null);
    }
}