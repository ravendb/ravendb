using System;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Data
{
	public class Attachment
	{
		public byte[] Data { get; set; }
		public JObject Metadata { get; set; }
		public Guid Etag { get; set; }
	}

	public class AttachmentInformation
	{
		public int Size { get; set; }
		public string Key { get; set; }
		public JObject Metadata { get; set; }
		public Guid Etag { get; set; }
	}
}