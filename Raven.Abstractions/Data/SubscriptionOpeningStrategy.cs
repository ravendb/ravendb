// -----------------------------------------------------------------------
//  <copyright file="SubscriptionOpeningStrategy.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Abstractions.Data
{
	/// <summary>
	/// Options for opening a subscription
	/// </summary>
	public enum SubscriptionOpeningStrategy
	{
		/// <summary>
		/// The first client will open a subscription and keeps it open as long as it sends 'client-alive' notifications.
		/// Other attempts to open the subscription will end up with SubscriptionInUseException.
		/// </summary>
		FirstKeepsOpen,
		/// <summary>
		/// The connecting client will successfully open a subscription even if there is another active subscription's consumer.
		/// The new client will take over the subscription while the existing one gets rejected. 
		/// The subscription will be always processed by the last connected client.
		/// </summary>
		LastTakesOver,
		/// <summary>
		/// The client opening a subscription with Forced strategy set, will always get it and keeps it open until another client with the same strategy gets connected.
		/// </summary>
		Forced
	}
}