//-----------------------------------------------------------------------
// <copyright file="StoredEntityEventArgs.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

namespace Raven.Client
{
	/// <summary>
	/// The event arguments raised when an entity is stored
	/// </summary>
	public class StoredEntityEventArgs : EventArgs
	{
		/// <summary>
		/// Gets or sets the session identifier.
		/// </summary>
		/// <value>The session identifier.</value>
		public string SessionIdentifier { get; set; }
		/// <summary>
		/// Gets or sets the entity instance.
		/// </summary>
		/// <value>The entity instance.</value>
		public object EntityInstance { get; set; }
	}
}