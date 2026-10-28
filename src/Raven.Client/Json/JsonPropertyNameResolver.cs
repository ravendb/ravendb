using System;
using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Newtonsoft.Json;
using Raven.Client.Documents.Conventions;
using Raven.Client.Json.Serialization.SystemTextJson;

namespace Raven.Client.Json
{
    internal static class JsonPropertyNameResolver
    {
        private static readonly ConcurrentDictionary<MemberInfo, string> _newtonsoftCache = new();
        private static readonly ConcurrentDictionary<MemberInfo, string> _systemTextJsonCache = new();

        public static string GetJsonPropertyName(MemberInfo member, DocumentConventions conventions)
        {
            if (conventions?.Serialization is SystemTextJsonSerializationConventions)
                return GetSystemTextJsonPropertyName(member);

            string result = _newtonsoftCache.GetOrAdd(member, ResolveNewtonsoftName);
            return result.Length == 0 ? null : result;
        }

        internal static string GetSystemTextJsonPropertyName(MemberInfo member)
        {
            string result = _systemTextJsonCache.GetOrAdd(member, ResolveSystemTextJsonName);
            return result.Length == 0 ? null : result;
        }

        private static string ResolveNewtonsoftName(MemberInfo member)
        {
            string name = member.GetCustomAttribute<JsonPropertyAttribute>(inherit: true)?.PropertyName;
            if (name != null)
                return name;

            return member.GetCustomAttribute<DataMemberAttribute>(inherit: true)?.Name ?? string.Empty;
        }

        private static string ResolveSystemTextJsonName(MemberInfo member)
        {
            return member.GetCustomAttribute<JsonPropertyNameAttribute>(inherit: true)?.Name
                   ?? member.GetCustomAttribute<JsonPropertyAttribute>(inherit: true)?.PropertyName
                   ?? GetDataContractMemberName(member)
                   ?? string.Empty;
        }

        private static string GetDataContractMemberName(MemberInfo member)
        {
            for (Type type = member.DeclaringType; type != null; type = type.BaseType)
            {
                if (type.IsDefined(typeof(DataContractAttribute), inherit: false))
                    return member.GetCustomAttribute<DataMemberAttribute>(inherit: true)?.Name;
            }

            return null;
        }
    }
}
