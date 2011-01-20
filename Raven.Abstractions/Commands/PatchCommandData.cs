//-----------------------------------------------------------------------
// <copyright file="PatchCommandData.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Newtonsoft.Json.Linq;
using Raven.Database.Json;
using System.Linq;
using Raven.Http;

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

		public JObject Metadata
		{
			get; set;
		}

        /// <summary>
		/// Translate this instance to a Json object.
		/// </summary>
		public JObject ToJson()
		{
			return new JObject(
				new JProperty("Key", Key),
				new JProperty("Method", Method),
				new JProperty("Etag", Etag == null ? null : new JValue(Etag.ToString())),
				new JProperty("Patches", new JArray(Patches.Select(x=>x.ToJson())))
				);
		}
	}
}
