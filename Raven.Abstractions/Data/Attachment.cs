//-----------------------------------------------------------------------
// <copyright file="Attachment.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using Raven.Json.Linq;

namespace Raven.Abstractions.Data
{
	/// <summary>
	/// Attachment data and metadata
	/// </summary>
	public class Attachment
	{
		/// <summary>
		/// Gets or sets the data.
		/// </summary>
		/// <value>The data.</value>
		public Func<Stream> Data { get; set; }

		/// <summary>
		/// The size of the attachment
		/// </summary>
		public int Size { get; set; }

		/// <summary>
		/// Gets or sets the metadata.
		/// </summary>
		/// <value>The metadata.</value>
		public RavenJObject Metadata { get; set; }
		/// <summary>
		/// Gets or sets the etag.
		/// </summary>
		/// <value>The etag.</value>
		public Guid Etag { get; set; }

		/// <summary>
		/// The attachment name
		/// </summary>
		public string Key { get; set; }
	}
}
