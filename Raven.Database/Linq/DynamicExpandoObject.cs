using System.Dynamic;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Linq
{
	public class DynamicJsonObject : DynamicObject
	{
		private readonly JObject obj;

		public DynamicJsonObject(JObject obj)
		{
			this.obj = obj;
		}

		public override bool TryGetMember(GetMemberBinder binder, out object result)
		{
			result = GetValue(binder.Name);
			return true;
		}

		private static object TransformToValue(JToken jToken)
		{
			switch (jToken.Type)
			{
				case JsonTokenType.Object:
					return new DynamicJsonObject((JObject) jToken);
				case JsonTokenType.Array:
					return jToken.Select(TransformToValue).ToArray();
				default:
					return jToken.Value<object>();
			}
		}

		public object GetValue(string name)
		{
			if (name == "__document_id")
			{
				return GetDocumentId();
			}
			JToken value;
			if (obj.TryGetValue(name, out value))
			{
				return TransformToValue(value);
			}
			return null;
		}

		private object GetDocumentId()
		{
			var metadata = obj["@metadata"];
			if (metadata != null)
			{
				var id = metadata["@id"];
				if (id != null)
				{
					return id.Value<object>();
				}
			}
			return null;
		}
	}
}