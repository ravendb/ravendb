using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Raven.Imports.Newtonsoft.Json;
using TypeScripter.Readers;

namespace TypingsGenerator
{
    public class TypeReaderWithIgnoreMethods : TypeReader
    {
        public override IEnumerable<MethodInfo> GetMethods(TypeInfo type)
        {
            return Enumerable.Empty<MethodInfo>();
        }

        public override IEnumerable<FieldInfo> GetFields(TypeInfo type)
        {
            return base.GetFields(type).Where(f => f.GetCustomAttribute<JsonIgnoreAttribute>() == null && f.GetCustomAttribute<Sparrow.Json.JsonIgnoreAttribute>() == null);
        }

        public override IEnumerable<PropertyInfo> GetProperties(TypeInfo type)
        {
            return base.GetProperties(type).Where(p => p.GetCustomAttribute<JsonIgnoreAttribute>() == null && p.GetCustomAttribute<Sparrow.Json.JsonIgnoreAttribute>() == null);
        }
    }
}