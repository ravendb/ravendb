//-----------------------------------------------------------------------
// <copyright file="TransactionInformation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

namespace Raven.Abstractions.Data
{
	/// <summary>
	/// Transaction information that identify the transaction id and timeout
	/// </summary>
	public class TransactionInformation
	{
		/// <summary>
		/// Gets or sets the id.
		/// </summary>
		/// <value>The id.</value>
        public string Id { get; set; }
		/// <summary>
		/// Gets or sets the timeout.
		/// </summary>
		/// <value>The timeout.</value>
		public TimeSpan Timeout { get; set; }
	}
}