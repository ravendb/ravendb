using System;
using System.IO;
using System.Text;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.RavenFS
{
	public class ServerInfo
	{
		public string FileSystemUrl { get; set; }

		public Guid Id { get; set; }

		public override string ToString()
		{
			return string.Format("{0} [{1}]", FileSystemUrl, Id);
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
