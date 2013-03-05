using System.Globalization;
using Raven.Abstractions.Spatial;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Client.Spatial
{
	public class SpatialClientShapeReader : AbstractSpatialShapeReader<string>
	{
		private readonly JsonSerializer jsonSerializer;

		public SpatialClientShapeReader(JsonSerializer jsonSerializer)
		{
			this.jsonSerializer = jsonSerializer;
		}

		public override bool TryRead(object value, out string result)
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

			var jValue = json as RavenJValue;
			if (jValue != null && jValue.Type == JTokenType.String)
			{
				result = (string) jValue.Value;
				return true;
			}

			return TryReadInner(json, out result);
		}

		protected override string MakePoint(double x, double y)
		{
			return string.Format(CultureInfo.InvariantCulture, "POINT ({0} {1})", x, y);
		}

		protected override string MakeCircle(double x, double y, double radius)
		{
			return string.Format(CultureInfo.InvariantCulture, "Circle({0} {1} d={2})", x, y, radius);
		}
	}
}
