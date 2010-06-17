using System;
using Newtonsoft.Json.Linq;

namespace Raven.Database
{
	public class BatchResult
	{
		public Guid? Etag { get; set; }
		public string Method { get; set; }
		public string Key { get; set; }
		public JObject Metadata { get; set; }
	}
}