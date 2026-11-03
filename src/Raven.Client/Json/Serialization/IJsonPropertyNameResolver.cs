using System.Reflection;

namespace Raven.Client.Json.Serialization
{
    internal interface IJsonPropertyNameResolver
    {
        string GetJsonPropertyName(MemberInfo member);
    }
}
