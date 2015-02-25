using System;
using System.Linq;
using System.Text;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Bundles.UniqueConstraints
{
	public static class Util
	{
		public static string EscapeUniqueValue(object value, bool caseInsensitive = false)
		{
			var stringToEscape = value.ToString();
            if (caseInsensitive)
		        stringToEscape = stringToEscape.ToLowerInvariant();
			var escapeDataString = Uri.EscapeDataString(stringToEscape);
			if (stringToEscape == escapeDataString)
				return stringToEscape;
			// to avoid issues with ids, we encode the entire thing as safe Base64
			return Convert.ToBase64String(Encoding.UTF8.GetBytes(stringToEscape));
		}

        public static UniqueConstraint GetConstraint(RavenJToken property)
        {
            switch (property.Type)
            {
                case JTokenType.String: // backward compatability
                    return new UniqueConstraint { PropName = property.Value<string>() };
                case JTokenType.Object:
                    return new UniqueConstraint { PropName = property.Value<string>("Name"), CaseInsensitive = property.Value<bool>("CaseInsensitive") };
                default:
                    throw new ArgumentOutOfRangeException(property.Type.ToString());
            }
        }


        public static bool TryGetUniqueValues(RavenJToken prop, out string[] uniqueValues)
        {
            if (prop == null || prop.Type == JTokenType.Null || (prop.Type == JTokenType.String && string.IsNullOrEmpty(prop.Value<string>())))
            {
                uniqueValues = null;
                return false;
            }

            var array = prop as RavenJArray;
            uniqueValues = array != null ? array.Select(p => p.Value<string>()).Where(x => !string.IsNullOrEmpty(x)).ToArray() : new[] { prop.Value<string>() };
            return true;
        }
	}

    public class UniqueConstraint
    {
        public string PropName { get; set; }
        public bool CaseInsensitive { get; set; }
    }
}