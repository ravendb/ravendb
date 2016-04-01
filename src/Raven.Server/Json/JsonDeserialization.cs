using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Raven.Abstractions.Data;
using Sparrow.Json;

namespace Raven.Server.Json
{
    public static class JsonDeserialization
    {
        public static readonly Func<BlittableJsonReaderObject, DatabaseDocument> DatabaseDocument = GenerateJsonDeserializationRoutine<DatabaseDocument>();

        public static Func<BlittableJsonReaderObject,T> GenerateJsonDeserializationRoutine<T>()
        {
            var json = Expression.Parameter(typeof(BlittableJsonReaderObject), "json");

            var vars = new Dictionary<Type, ParameterExpression>();
            var instance = Expression.New(typeof(T).GetConstructor(new Type[0]));
            var propInit = new List<MemberBinding>();
            foreach (var propertyInfo in typeof(T).GetProperties())
            {
                propInit.Add(Expression.Bind(propertyInfo,GetValue(propertyInfo, json, vars)));
            }

            var lambda = Expression.Lambda<Func<BlittableJsonReaderObject, T>>(Expression.Block(vars.Values, Expression.MemberInit(instance, propInit)), json);

            return lambda.Compile();
        }

        private static Expression GetValue(PropertyInfo propertyInfo, ParameterExpression json, Dictionary<Type, ParameterExpression> vars)
        {
            if (propertyInfo.PropertyType == typeof (Dictionary<string, string>))
            {
                return Expression.Call(typeof (JsonDeserialization).GetMethod(nameof(ToDictionary)),
                    json, Expression.Constant(propertyInfo.Name));

            }
            var value = GetParameter(propertyInfo.PropertyType, vars);

            var genericType = propertyInfo.PropertyType != typeof (string) ? new[] {propertyInfo.PropertyType} : new Type[0];

            var tryGet = Expression.Call(json, "TryGet", genericType, Expression.Constant(propertyInfo.Name), value);
            return Expression.Condition(tryGet, value, Expression.Default(propertyInfo.PropertyType));
        }

        private static ParameterExpression GetParameter(Type type, Dictionary<Type, ParameterExpression> vars)
        {
            ParameterExpression value;
            if (vars.TryGetValue(type, out value) == false)
            {
                value = Expression.Variable(type, type.Name);
                vars[type] = value;
            }
            return value;
        }

        public static Dictionary<string, string> ToDictionary(BlittableJsonReaderObject json, string name)
        {
            var dic = new Dictionary<string,string>(StringComparer.OrdinalIgnoreCase);

            BlittableJsonReaderObject obj;
            if (json.TryGet(name, out obj) == false)
                return dic;

            foreach (var propertyName in obj.GetPropertyNames())
            {
                string val;
                if (obj.TryGet(propertyName, out val))
                {
                    dic[propertyName] = val;
                }
            }
            return dic;
        } 
    }
}