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
        /// If the new client takes over the subscription then the existing one will get rejected. 
        /// The subscription will always be processed by the last connected client.
        /// </summary>
        TakeOver,
        /// <summary>
        /// The client opening a subscription with Forced strategy set will always get it and keep it open until another client with the same strategy gets connected.
        /// </summary>
        ForceAndKeep,
        /// <summary>
        /// If the client currently cannot open the subscription because it is used by another client then it will subscribe Changes API to be notified about subscription status changes.
        /// Every time SubscriptionReleased notification arrives, it will repeat an attempt to open the subscription. After it succeeds in opening, it will process docs as usual.
        /// </summary>
        WaitForFree
    }
}
