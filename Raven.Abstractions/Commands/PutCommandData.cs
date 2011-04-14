//-----------------------------------------------------------------------
// <copyright file="PutCommandData.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Abstractions.Commands
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
    		var ret = new RavenJObject
    		          	{
    		          		{"Key", Key},
							{"Method", Method},
							{"Document", Document},
							{"Metadata", Metadata}
    		          	};
			if (Etag != null)
				ret.Add("Etag", Etag.ToString());
    		return ret;
    	}
    }
}
