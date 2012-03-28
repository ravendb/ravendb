using System;
using System.Collections.Specialized;
using System.IO;

namespace Raven.VisualHost
{
	public class TrackedRequest
	{
		public string Method { get; set; }
		public string Url { get; set; }

		public NameValueCollection RequestHeaders { get; set; }
		public Stream RequestContent { get; set; }

		public NameValueCollection ResponseHeaders { get; set; }
		public Stream ResponseContent { get; set; }

		public int Status { get; set; }
	}

}