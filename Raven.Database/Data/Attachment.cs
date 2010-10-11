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
}
