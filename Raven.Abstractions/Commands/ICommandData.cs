//-----------------------------------------------------------------------
// <copyright file="ICommandData.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Newtonsoft.Json.Linq;
using Raven.Http;

namespace Raven.Database.Data
{
	/// <summary>
	/// A single operation inside a batch
	/// </summary>
    public interface ICommandData
    {
		/// <summary>
		/// Gets the key.
		/// </summary>
		/// <value>The key.</value>
		string Key { get; }
		/// <summary>
		/// Gets the method.
		/// </summary>
		/// <value>The method.</value>
		string Method { get; }
		/// <summary>
		/// Gets the etag.
		/// </summary>
		/// <value>The etag.</value>
		Guid? Etag { get; }

		TransactionInformation TransactionInformation { get; set; }
    	JObject Metadata { get; }

        /// <summary>
		/// Translate this instance to a Json object.
		/// </summary>
		JObject ToJson();
    }
}
