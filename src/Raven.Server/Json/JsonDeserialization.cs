using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Raven.Abstractions.Data;

namespace Raven.Server.Json
{
    //TODO: Fix this with a cache using Expression values to avoid using reflection
    public static class JsonDeserialization
    {
        public static readonly Func<BlittableJsonReaderObject, DatabaseDocument> DatabaseDocument = GenerateJsonDeserializationRoutine<DatabaseDocument>();

        public static Func<BlittableJsonReaderObject,T> GenerateJsonDeserializationRoutine<T>()
        {
            var json = Expression.Parameter(typeof(BlittableJsonReaderObject), "json");

            // new T();
            
            var vars = new Dictionary<Type, ParameterExpression>();
            var instance = Expression.New(typeof(T).GetConstructor(new Type[0]));
            var propInit = new List<MemberBinding>();
            foreach (var propertyInfo in typeof(T).GetProperties())
            {
                ParameterExpression value;
                if (vars.TryGetValue(propertyInfo.PropertyType, out value) == false)
                {
                    value = Expression.Variable(propertyInfo.PropertyType, propertyInfo.PropertyType.Name);
                    vars[propertyInfo.PropertyType] = value;
                }

                var genericType = propertyInfo.PropertyType != typeof(string) ? new[] { propertyInfo.PropertyType } : new Type[0];

                var tryGet = Expression.Call(json, "TryGet", genericType, Expression.Constant(propertyInfo.Name), value);

                var conditionalExpression = Expression.Condition(tryGet, value, Expression.PropertyOrField(instance, propertyInfo.Name));


                propInit.Add(Expression.Bind(propertyInfo,conditionalExpression));
            }

            var lambda = Expression.Lambda<Func<BlittableJsonReaderObject, T>>(Expression.Block(vars.Values, Expression.MemberInit(instance, propInit)), json);

            return lambda.Compile();
        }


        private static T Deserialize<T>(BlittableJsonReaderObject obj) where T : new()
        {
            var deserialize = new T();
            Populate(obj, deserialize, typeof(T));
            return deserialize;
        }

        private static object Deserialize(BlittableJsonReaderObject obj, Type type)
        {
            var deserialize = Activator.CreateInstance(type);
            return Populate(obj, deserialize, type);
        }

        private static object Populate(BlittableJsonReaderObject obj, object deserialize, Type type)
        {
            foreach (var propertyName in obj.GetPropertyNames())
            {
                object result;
                obj.TryGetMember(propertyName, out result);

                var str = result as LazyStringValue;
                if (type == typeof(Dictionary<string, string>))
                {
                    var dictionary = (Dictionary<string, string>)deserialize;
                    dictionary[propertyName] = str;
                    continue;
                }
                var propertyInfo = type.GetProperty(propertyName);
                if (str != null)
                {
                    propertyInfo.SetValue(deserialize, (string)str);
                    continue;
                }
                var jsonReaderObject = result as BlittableJsonReaderObject;
                if (jsonReaderObject != null)
                {
                    var value = Deserialize(jsonReaderObject, propertyInfo.PropertyType);
                    propertyInfo.SetValue(deserialize, value);
                }
                else
                {
                    propertyInfo.SetValue(deserialize, result);
                }
            }
            return deserialize;
        }
    }
}