// -----------------------------------------------------------------------
//  <copyright file="SubscriptionOpeningStrategy.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Client.Documents.Subscriptions
{
    /// <summary>
    /// Options for opening a subscription
    /// </summary>
    public enum SubscriptionOpeningStrategy
    {
        /// <summary>
        /// The client will successfully open a subscription only if there isn't any other currently connected client. 
        /// Otherwise it will end up with SubscriptionInUseException.
        /// </summary>
        OpenIfFree,
        /// <summary>
        /// The connecting client will successfully open a subscription even if there is another active subscription's consumer.
        /// If the new client takes over an existing client then the existing one will get a SubscriptionInUseException.
        ///  
        /// The subscription will always be held by the last connected client.
        /// </summary>
        TakeOver,
        /// <summary>
        /// If the client currently cannot open the subscription because it is used by another client but it will wait for that client
        /// to complete and keep attempting to gain the subscription
        /// </summary>
        WaitForFree,

        Concurrent
    }
}
