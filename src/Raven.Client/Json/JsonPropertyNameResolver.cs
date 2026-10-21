using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.Serialization;

namespace Raven.Client.Json
{
    internal static class JsonPropertyNameResolver
    {
        private static readonly ConcurrentDictionary<MemberInfo, string> _cache = new();

        public static string GetJsonPropertyName(MemberInfo member)
        {
            var result = _cache.GetOrAdd(member, ResolveJsonPropertyName);
            return result.Length == 0 ? null : result;
        }

        private static string ResolveJsonPropertyName(MemberInfo member)
        {
            foreach (var attr in member.GetCustomAttributes(true))
            {
                var attrType = attr.GetType();

                if (attrType.Namespace == "Newtonsoft.Json" && attrType.Name == "JsonPropertyAttribute")
                {
                    var propertyName = ((dynamic)attr).PropertyName as string;
                    if (propertyName != null)
                        return propertyName;
                }

                if (attrType.Namespace == "System.Text.Json.Serialization" && attrType.Name == "JsonPropertyNameAttribute")
                {
                    var name = ((dynamic)attr).Name as string;
                    if (name != null)
                        return name;
                }

                if (attr is DataMemberAttribute dataMember && dataMember.Name != null)
                    return dataMember.Name;
            }
            return string.Empty;
        }
    }
}
