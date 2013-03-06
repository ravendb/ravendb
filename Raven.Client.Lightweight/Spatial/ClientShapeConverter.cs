using Raven.Abstractions.Spatial;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Client.Spatial
{
	public class ClientShapeConverter : AbstractShapeConverter
	{
		private readonly JsonSerializer jsonSerializer;

		public ClientShapeConverter(JsonSerializer jsonSerializer)
		{
			this.jsonSerializer = jsonSerializer;
		}

		public override bool TryConvert(object value, out string result)
		{
			var wkt = value as string;
			if (wkt != null)
			{
				result = wkt;
				return true;
			}

			RavenJToken json;
			using (var jsonWriter = new RavenJTokenWriter())
			{
				jsonSerializer.Serialize(jsonWriter, value);
				json = jsonWriter.Token;
			}

			return TryConvertInner(json, out result);
		}
	}
}
