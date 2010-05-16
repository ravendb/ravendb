using System.Dynamic;
using System.Linq;
using Newtonsoft.Json.Linq;
using Raven.Database.Json;

namespace Raven.Database.Linq
{
	public class DynamicJsonObject : DynamicObject
	{
		private readonly JObject obj;

	    public JObject Inner
	    {
	        get { return obj; }
	    }

	    public DynamicJsonObject(JObject obj)
		{
			this.obj = obj;
		}

		public override bool TryGetMember(GetMemberBinder binder, out object result)
		{
			result = GetValue(binder.Name);
			return true;
		}

		public override bool TryGetIndex(GetIndexBinder binder, object[] indexes, out object result)
		{
			if (indexes.Length != 1 || indexes[0] is string == false)
			{
				result = null;
				return false;
			}
			result = GetValue((string)indexes[0]);
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
					var value = jToken.Value<object>();
					if(value is long)
					{
						var l = (long) value;
						if(l > int.MinValue && int.MaxValue > l)
							return (int) l;
					}
					var str = value as string;
					if(str != null && str.StartsWith("0x"))
					{
						return JsonLuceneNumberConverter.ParseNumber(str);
					}
					return value;
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

	    public string GetDocumentId()
		{
			var metadata = obj["@metadata"];
			if (metadata != null)
			{
				var id = metadata["@id"];
				if (id != null)
				{
					return id.Value<string>();
				}
			}
			return null;
		}
	}
}