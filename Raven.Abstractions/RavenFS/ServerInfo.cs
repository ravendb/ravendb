using System;
using System.IO;
using System.Text;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.RavenFS
{
	public class ServerInfo
	{
		public string Url { get; set; } // TODO arek - assign urls + file system names

		public Guid Id { get; set; }

		public override string ToString()
		{
			return string.Format("{0} [{1}]", Url, Id);
		}

		public string AsJson()
		{
			var sb = new StringBuilder();
			var jw = new JsonTextWriter(new StringWriter(sb));
			new JsonSerializer().Serialize(jw, this);
			return sb.ToString();
		}
	}
}
