//-----------------------------------------------------------------------
// <copyright file="ICommandData.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Abstractions.Commands
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
		Etag Etag { get; }

		/// <summary>
		/// Gets the transaction information.
		/// </summary>
		/// <value>The transaction information.</value>
		TransactionInformation TransactionInformation { get; set; }

		/// <summary>
		/// Gets the metadata.
		/// </summary>
		/// <value>The metadata.</value>
		RavenJObject Metadata { get; }

		/// <summary>
		/// Gets the Additional Data.
		/// </summary>
		/// <value>The Additional Data.</value>
		RavenJObject AdditionalData { get; set; }

		/// <summary>
		/// Translate this instance to a Json object.
		/// </summary>
		RavenJObject ToJson();
	}
}