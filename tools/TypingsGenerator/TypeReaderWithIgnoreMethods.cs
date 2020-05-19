using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Newtonsoft.Json;
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
            return base.GetFields(type)
                .Where(f => f.GetCustomAttribute<JsonIgnoreAttribute>() == null && f.GetCustomAttribute<Sparrow.Json.JsonDeserializationIgnoreAttribute>() == null);
        }

        public override IEnumerable<PropertyInfo> GetProperties(TypeInfo type)
        {
            return base.GetProperties(type).Where(p =>
                p.GetCustomAttribute<JsonIgnoreAttribute>() == null
                && p.GetCustomAttribute<Sparrow.Json.JsonDeserializationIgnoreAttribute>() == null
                && !IsDictionaryIndexer(p)
                && !IsFunctionProperty(p));
        }

        private Boolean IsFunctionProperty(PropertyInfo propertyInfo)
        {
            return propertyInfo.PropertyType.IsGenericType && typeof(Func<>).IsAssignableFrom(propertyInfo.PropertyType.GetGenericTypeDefinition());
        }

        private bool IsDictionaryIndexer(PropertyInfo propertyInfo)
        {
            var isDict = typeof(IDictionary).IsAssignableFrom(propertyInfo.DeclaringType);
            return isDict && propertyInfo.Name == "Item";
        }
    }
}