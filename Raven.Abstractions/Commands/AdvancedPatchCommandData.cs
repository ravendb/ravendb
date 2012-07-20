//-----------------------------------------------------------------------
// <copyright file="PatchCommandData.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Abstractions.Extensions;

namespace Raven.Abstractions.Commands
{
	///<summary>
	/// A single batch operation for a document PATCH (using a Javascript)
	///</summary>
	public class AdvancedPatchCommandData : ICommandData
	{
		/// <summary>
		/// Gets or sets the AdvancedPatchRequest (using JavaScript) that is used to patch the document
		/// </summary>
		/// <value>The Script.</value>
		public AdvancedPatchRequest Patch 
		{ 
			get; set; 
		}

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
			get { return "ADVANCEDPATCH"; }
		}

		/// <summary>
		/// Gets or sets the etag.
		/// </summary>
		/// <value>The etag.</value>
		public Guid? Etag
		{
			get; set;
		}

		public TransactionInformation TransactionInformation
		{
			get; set;
		}

		public RavenJObject Metadata
		{
			get; set;
		}

		/// <summary>
		/// Translate this instance to a Json object.
		/// </summary>
		public RavenJObject ToJson()
		{
			var ret = new RavenJObject
					{
						{"Key", Key},
						{"Method", Method},
						{"Patch", new RavenJObject
						{
							{ "Script", Patch.Script },
							{ "PrevVal", Patch.PrevVal },
						}}
					};
			if (Etag != null)
				ret.Add("Etag", Etag.ToString());
			return ret;
		}
	}
}
