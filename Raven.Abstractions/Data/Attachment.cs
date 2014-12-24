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
	[Obsolete("Use RavenFS instead.")]
	public class Attachment
	{
		/// <summary>
		/// Function returning the content of an attachment.
		/// </summary>
		public Func<Stream> Data { get; set; }

		/// <summary>
		/// Attachment size in bytes.
		/// </summary>
		/// <remarks>The max size of an attachment can be 2GB.</remarks>
		public int Size { get; set; }

		/// <summary>
		/// RavenJObject representing attachment's metadata.
		/// </summary>
		public RavenJObject Metadata { get; set; }

		/// <summary>
		/// Current attachment etag, used for concurrency checks (null to skip check)
		/// </summary>
		public Etag Etag { get; set; }

		/// <summary>
		/// Key of an attachment.
		/// </summary>
		public string Key { get; set; }
	}
}
