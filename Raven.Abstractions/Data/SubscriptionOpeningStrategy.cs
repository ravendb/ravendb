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
		/// The client will successfully open a subscription only if there isn't any another client currently connected. 
		/// Otherwise it will end up with SubscriptionInUseException.
		/// </summary>
		OpenIfFree,
		/// <summary>
		/// The connecting client will successfully open a subscription even if there is another active subscription's consumer.
		/// The new client will take over the subscription while the existing one gets rejected. 
		/// The subscription will be always processed by the last connected client.
		/// </summary>
		TakeOver,
		/// <summary>
		/// The client opening a subscription with Forced strategy set, will always get it and keeps it open until another client with the same strategy gets connected.
		/// </summary>
		Force
	}
}