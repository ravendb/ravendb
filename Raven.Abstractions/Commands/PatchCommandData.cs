//-----------------------------------------------------------------------
// <copyright file="PatchCommandData.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Database.Json;
using System.Linq;
using Raven.Http;
using Raven.Json.Linq;

namespace Raven.Database.Data
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
		/// Gets the etag.
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
        	var ret = new RavenJObject();
        	ret.AddValueProperty("Key", Key);
        	ret.AddValueProperty("Method", Method);
			if (Etag != null)
        		ret.Properties.Add("Etag", new RavenJValue(Etag.ToString()));
        	ret.Properties.Add("Patches", new RavenJArray(Patches.Select(x => x.ToJson())));
        	return ret;
        }
	}
}
