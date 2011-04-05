//-----------------------------------------------------------------------
// <copyright file="PutCommandData.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Http;
using Raven.Json.Linq;

namespace Raven.Database.Data
{
	/// <summary>
	/// A single batch operation for a document PUT
	/// </summary>
    public class PutCommandData : ICommandData
    {
		/// <summary>
		/// Gets or sets the key.
		/// </summary>
		/// <value>The key.</value>
        public virtual string Key { get; set; }

		/// <summary>
		/// Gets the method.
		/// </summary>
		/// <value>The method.</value>
    	public string Method
    	{
			get { return "PUT"; }
    	}

    	public TransactionInformation TransactionInformation
        {
            get;
            set;
        }

		/// <summary>
		/// Gets or sets the etag.
		/// </summary>
		/// <value>The etag.</value>
        public virtual Guid? Etag { get; set; }
		/// <summary>
		/// Gets or sets the document.
		/// </summary>
		/// <value>The document.</value>
        public virtual RavenJObject Document { get; set; }
		/// <summary>
		/// Gets or sets the metadata.
		/// </summary>
		/// <value>The metadata.</value>
        public virtual RavenJObject Metadata { get; set; }


    	/// <summary>
		/// Translate this instance to a Json object.
		/// </summary>
		public RavenJObject ToJson()
    	{
    		var ret = new RavenJObject();
			ret.Properties.Add("Key", new RavenJValue((object)Key));
			ret.Properties.Add("Etag", new RavenJValue(Etag != null ? (object)Etag.ToString() : null));
			ret.Properties.Add("Method", new RavenJValue(Method));
    		ret.Properties.Add("Document", Document);
    		ret.Properties.Add("Metadata", Metadata);
    		return ret;
    	}
    }
}
