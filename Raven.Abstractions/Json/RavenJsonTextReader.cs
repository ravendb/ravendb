using System.IO;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.Json
{
	public class RavenJsonTextReader : JsonTextReader
	{
		public RavenJsonTextReader(TextReader reader)
			: base(reader)
		{
			DateParseHandling = DateParseHandling.None;
		}
	}
}