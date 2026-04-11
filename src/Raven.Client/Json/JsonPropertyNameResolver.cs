using System.Reflection;
using System.Runtime.Serialization;

namespace Raven.Client.Json
{
    internal static class JsonPropertyNameResolver
    {
        /// <summary>
        /// Gets the JSON property name for a member by checking:
        /// 1. Newtonsoft.Json.JsonPropertyAttribute.PropertyName (by namespace, to avoid hard dependency)
        /// 2. System.Text.Json.Serialization.JsonPropertyNameAttribute.Name
        /// 3. System.Runtime.Serialization.DataMemberAttribute.Name
        /// Returns null if no custom name is found.
        /// </summary>
        public static string GetJsonPropertyName(MemberInfo member)
        {
            foreach (var attr in member.GetCustomAttributes(false))
            {
                var attrType = attr.GetType();

                // Check Newtonsoft.Json.JsonPropertyAttribute by namespace (avoid hard coupling)
                if (attrType.Namespace == "Newtonsoft.Json" && attrType.Name == "JsonPropertyAttribute")
                {
                    var propertyName = ((dynamic)attr).PropertyName as string;
                    if (propertyName != null)
                        return propertyName;
                }

                // Check System.Text.Json.Serialization.JsonPropertyNameAttribute (by namespace, to avoid hard coupling with older TFMs)
                if (attrType.Namespace == "System.Text.Json.Serialization" && attrType.Name == "JsonPropertyNameAttribute")
                {
                    var name = ((dynamic)attr).Name as string;
                    if (name != null)
                        return name;
                }

                // Check DataMemberAttribute
                if (attr is DataMemberAttribute dataMember && dataMember.Name != null)
                    return dataMember.Name;
            }
            return null;
        }
    }
}
