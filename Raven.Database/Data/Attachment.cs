using System;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Data
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
		public byte[] Data { get; set; }
		/// <summary>
		/// Gets or sets the metadata.
		/// </summary>
		/// <value>The metadata.</value>
		public JObject Metadata { get; set; }
		/// <summary>
		/// Gets or sets the etag.
		/// </summary>
		/// <value>The etag.</value>
		public Guid Etag { get; set; }
	}

	/// <summary>
	/// Describe an attachment, but without the actual attachment data
	/// </summary>
	public class AttachmentInformation
	{
		/// <summary>
		/// Gets or sets the size.
		/// </summary>
		/// <value>The size.</value>
		public int Size { get; set; }
		/// <summary>
		/// Gets or sets the key.
		/// </summary>
		/// <value>The key.</value>
		public string Key { get; set; }
		/// <summary>
		/// Gets or sets the metadata.
		/// </summary>
		/// <value>The metadata.</value>
		public JObject Metadata { get; set; }
		/// <summary>
		/// Gets or sets the etag.
		/// </summary>
		/// <value>The etag.</value>
		public Guid Etag { get; set; }
	}
}
