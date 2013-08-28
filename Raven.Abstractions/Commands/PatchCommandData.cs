//-----------------------------------------------------------------------
// <copyright file="PatchCommandData.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Abstractions.Commands
{
	///<summary>
	/// A single batch operation for a document PATCH
	///</summary>
	public class PatchCommandData : ICommandData
	{
		/// <summary>
		/// Gets or sets the patches applied to this document
		/// </summary>
		/// <value>The patches.</value>
		public PatchRequest[] Patches{ get; set;}

		/// <summary>
		/// Gets or sets the patches to apply to a default document if the document is missing
		/// </summary>
		/// <value>The patches.</value>
		public PatchRequest[] PatchesIfMissing { get; set; }

		/// <summary>
		/// Gets the key.
		/// </summary>
		/// <value>The key.</value>
		public string Key
		{
			get; set;
		}

		/// <summary>
		/// Gets the method.
		/// </summary>
		/// <value>The method.</value>
		public string Method
		{
			get { return "PATCH"; }
		}

		/// <summary>
		/// Gets or sets the etag.
		/// </summary>
		/// <value>The etag.</value>
		public Etag Etag
		{
			get; set;
		}

		/// <summary>
		/// Gets the transaction information.
		/// </summary>
		/// <value>The transaction information.</value>
		public TransactionInformation TransactionInformation
		{
			get; set;
		}

		/// <summary>
		/// Gets or sets the metadata.
		/// </summary>
		/// <value>The metadata.</value>
		public RavenJObject Metadata
		{
			get; set;
		}

		public RavenJObject AdditionalData { get; set; }

		/// <summary>
		/// Translate this instance to a Json object.
		/// </summary>
		public RavenJObject ToJson()
		{
			var ret = new RavenJObject
			       	{
						{"Key", Key},
						{"Method", Method},
						{"Patches", new RavenJArray(Patches.Select(x => x.ToJson()))},
						{"Metadata", Metadata},
						{"AdditionalData", AdditionalData}
					};
			if (Etag != null)
				ret.Add("Etag", Etag.ToString());
			if (PatchesIfMissing != null && PatchesIfMissing.Length > 0)
				ret.Add("PatchesIfMissing", new RavenJArray(PatchesIfMissing.Select(x => x.ToJson())));
			return ret;
		}
	}
}