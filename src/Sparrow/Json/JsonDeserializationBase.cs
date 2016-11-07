using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Sparrow.Json
{
    public class JsonDeserializationBase
    {
        private static readonly Type[] EmptyTypes = new Type[0];
        private static readonly Dictionary<Type, object> DeserializedTypes = new Dictionary<Type, object>();

        protected static Func<BlittableJsonReaderObject, T> GenerateJsonDeserializationRoutine<T>()
        {
            try
            {
                var type = typeof(T);
                var json = Expression.Parameter(typeof(BlittableJsonReaderObject), "json");

                var vars = new Dictionary<Type, ParameterExpression>();

                if (type == typeof(BlittableJsonReaderArray))
                {
                    return null;
                }

                var ctor = type.GetConstructor(EmptyTypes);
                var instance = ctor != null
                    ? Expression.New(ctor)
                    : Expression.New(type);

                if (typeof(T).GetInterfaces().Contains(typeof(IFillFromBlittableJson)))
                {
                    var obj = Expression.Parameter(type, "obj");
                    var methodToCall = typeof(IFillFromBlittableJson).GetMethod(nameof(IFillFromBlittableJson.FillFromBlittableJson), BindingFlags.Public | BindingFlags.Instance);

                    var returnTarget = Expression.Label(type);

                    var block = Expression.Block(
                        new[] { obj },
                        Expression.Assign(obj, Expression.MemberInit(instance)),
                        Expression.Call(obj, methodToCall, json),
                        Expression.Return(returnTarget, obj, type),
                        Expression.Label(returnTarget, Expression.Default(type))
                    );

                    var l = Expression.Lambda<Func<BlittableJsonReaderObject, T>>(block, json);
                    return l.Compile();
                }

                var propInit = new List<MemberBinding>();
                foreach (var fieldInfo in typeof(T).GetFields())
                {
                    if (fieldInfo.IsStatic || fieldInfo.IsDefined(typeof(JsonIgnoreAttribute)))
                        continue;
                    propInit.Add(Expression.Bind(fieldInfo, GetValue(fieldInfo.Name, fieldInfo.FieldType, json, vars)));
                }
                foreach (var propertyInfo in typeof(T).GetProperties())
                {
                    if (propertyInfo.CanWrite == false || propertyInfo.IsDefined(typeof(JsonIgnoreAttribute)))
                        continue;
                    propInit.Add(Expression.Bind(propertyInfo, GetValue(propertyInfo.Name, propertyInfo.PropertyType, json, vars)));
                }

                var lambda = Expression.Lambda<Func<BlittableJsonReaderObject, T>>(Expression.Block(vars.Values, Expression.MemberInit(instance, propInit)), json);
                return lambda.Compile();
            }
            catch (Exception e)
            {
                return o =>
                {
                    throw new InvalidOperationException($"Could not build json parser for {typeof(T).FullName}", e);
                };
            }
        }

        private static Expression GetValue(string propertyName, Type propertyType, ParameterExpression json, Dictionary<Type, ParameterExpression> vars)
        {
            var type = Nullable.GetUnderlyingType(propertyType) ?? propertyType;
            if (type == typeof(string) ||
                type == typeof(bool) ||
                type == typeof(long) ||
                type == typeof(int) ||
                type == typeof(double) ||
                type.GetTypeInfo().IsEnum ||
                type == typeof(Guid) ||
                type == typeof(DateTime) ||
                type == typeof(BlittableJsonReaderArray) ||
                type == typeof(BlittableJsonReaderObject))
            {
                var value = GetParameter(propertyType, vars);

                Type[] genericTypes;
                if (type == typeof(string) || type == typeof(double)) // we support direct conversion to these types
                    genericTypes = EmptyTypes;
                else
                    genericTypes = new[] { propertyType };

                var tryGet = Expression.Call(json, nameof(BlittableJsonReaderObject.TryGet), genericTypes, Expression.Constant(propertyName), value);
                return Expression.Condition(tryGet, value, Expression.Default(propertyType));
            }

            if (propertyType.GetTypeInfo().IsGenericType)
            {
                var genericTypeDefinition = propertyType.GetGenericTypeDefinition();
                if (genericTypeDefinition == typeof(Dictionary<,>))
                {
                    var valueType = propertyType.GenericTypeArguments[1];
                    if (valueType == typeof(string))
                    {
                        var methodToCall = typeof(JsonDeserializationBase).GetMethod(nameof(ToDictionaryOfString), BindingFlags.NonPublic | BindingFlags.Static);
                        return Expression.Call(methodToCall, json, Expression.Constant(propertyName));
                    }
                    if (valueType == typeof(Dictionary<string, string[]>))
                    {
                        var methodToCall = typeof(JsonDeserializationBase).GetMethod(nameof(ToDictionaryOfDictionaryOfStringArray), BindingFlags.NonPublic | BindingFlags.Static);
                        return Expression.Call(methodToCall, json, Expression.Constant(propertyName));
                    }
                    if (valueType == typeof(string[]))
                    {
                        var methodToCall = typeof(JsonDeserializationBase).GetMethod(nameof(ToDictionaryOfStringArray), BindingFlags.NonPublic | BindingFlags.Static);
                        return Expression.Call(methodToCall, json, Expression.Constant(propertyName));
                    }
                    if (valueType.GetTypeInfo().IsEnum)
                    {
                        var methodToCall = typeof(JsonDeserializationBase).GetMethod(nameof(ToDictionaryOfEnum), BindingFlags.NonPublic | BindingFlags.Static);
                        methodToCall = methodToCall.MakeGenericMethod(valueType);
                        return Expression.Call(methodToCall, json, Expression.Constant(propertyName));
                    }
                    else
                    {
                        var converterExpression = Expression.Constant(GetConverterFromCache(valueType));
                        var methodToCall = typeof(JsonDeserializationBase).GetMethod(nameof(ToDictionary), BindingFlags.NonPublic | BindingFlags.Static).MakeGenericMethod(valueType);
                        return Expression.Call(methodToCall, json, Expression.Constant(propertyName), converterExpression);
                    }
                }

                if (propertyType == typeof(List<string>) || propertyType == typeof(HashSet<string>))
                {
                    var method = typeof(JsonDeserializationBase).GetMethod(nameof(ToCollectionOfString), BindingFlags.NonPublic | BindingFlags.Static);
                    method = method.MakeGenericMethod(propertyType);
                    return Expression.Call(method, json, Expression.Constant(propertyName));
                }

                if (genericTypeDefinition == typeof(List<>))
                {
                    var valueType = propertyType.GenericTypeArguments[0];
                    var converterExpression = Expression.Constant(GetConverterFromCache(valueType));
                    var methodToCall = typeof(JsonDeserializationBase).GetMethod(nameof(ToList), BindingFlags.NonPublic | BindingFlags.Static).MakeGenericMethod(valueType);
                    return Expression.Call(methodToCall, json, Expression.Constant(propertyName), converterExpression);
                }
            }

            // Ignore types
            /*if (type == typeof(IDisposable))
            {
                return Expression.Default(type);
            }*/
            if (propertyType == typeof(string[]))
            {
                var method = typeof(JsonDeserializationBase).GetMethod(nameof(ToArrayOfString), BindingFlags.NonPublic | BindingFlags.Static);
                return Expression.Call(method, json, Expression.Constant(propertyName));
            }
            if (propertyType.IsArray)
            {
                var valueType = propertyType.GetElementType();
                var converterExpression = Expression.Constant(GetConverterFromCache(valueType));
                var methodToCall = typeof(JsonDeserializationBase).GetMethod(nameof(ToArray), BindingFlags.NonPublic | BindingFlags.Static).MakeGenericMethod(valueType);
                return Expression.Call(methodToCall, json, Expression.Constant(propertyName), converterExpression);
            }

            // ToObject
            {
                var converterExpression = Expression.Constant(GetConverterFromCache(propertyType));
                var methodToCall = typeof(JsonDeserializationBase).GetMethod(nameof(ToObject), BindingFlags.NonPublic | BindingFlags.Static).MakeGenericMethod(propertyType);
                return Expression.Call(methodToCall, json, Expression.Constant(propertyName), converterExpression);
            }

            // throw new InvalidOperationException($"We weren't able to convert the property '{propertyName}' of type '{type}'.");
        }

        private static object GetConverterFromCache(Type propertyType)
        {
            object converter;
            if (DeserializedTypes.TryGetValue(propertyType, out converter) == false)
            {
                DeserializedTypes[propertyType] = converter = typeof(JsonDeserializationBase)
                    .GetMethod(nameof(GenerateJsonDeserializationRoutine), BindingFlags.NonPublic | BindingFlags.Static)
                    .MakeGenericMethod(propertyType)
                    .Invoke(null, null);
            }
            return converter;
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

        private static Dictionary<string, T> ToDictionary<T>(BlittableJsonReaderObject json, string name, Func<BlittableJsonReaderObject, T> converter)
        {
            var dic = new Dictionary<string, T>(StringComparer.OrdinalIgnoreCase);

            BlittableJsonReaderObject obj;
            if (json.TryGet(name, out obj) == false || obj == null)
                return dic;

            foreach (var propertyName in obj.GetPropertyNames())
            {
                object val;
                if (obj.TryGetMember(propertyName, out val))
                {
                    dic[propertyName] = converter((BlittableJsonReaderObject)val);
                }
            }
            return dic;
        }

        private static Dictionary<string, TEnum> ToDictionaryOfEnum<TEnum>(BlittableJsonReaderObject json, string name)
        {
            var dic = new Dictionary<string, TEnum>(StringComparer.OrdinalIgnoreCase);

            BlittableJsonReaderObject obj;
            //should a "null" exist in json? -> not sure that "null" can exist there
            if (json.TryGet(name, out obj) == false || obj == null)
                return dic;

            foreach (var propertyName in obj.GetPropertyNames())
            {
                string val;
                if (obj.TryGet(propertyName, out val))
                {
                    dic[propertyName] = (TEnum)Enum.Parse(typeof(TEnum), val, true);
                }
            }
            return dic;
        }

        private static Dictionary<string, string> ToDictionaryOfString(BlittableJsonReaderObject json, string name)
        {
            var dic = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            BlittableJsonReaderObject obj;
            //should a "null" exist in json? -> not sure that "null" can exist there
            if (json.TryGet(name, out obj) == false || obj == null)
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

        private static Dictionary<string, string[]> ToDictionaryOfStringArray(BlittableJsonReaderObject json, string name)
        {
            var dic = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);

            BlittableJsonReaderObject obj;
            //should a "null" exist in json? -> not sure that "null" can exist there
            if (json.TryGet(name, out obj) == false || obj == null)
                return dic;

            foreach (var propertyName in obj.GetPropertyNames())
            {
                BlittableJsonReaderArray val;
                if (obj.TryGet(propertyName, out val))
                {
                    var array = new string[val.Length];
                    for (int i = 0; i < val.Length; i++)
                    {
                        array[i] = val[i]?.ToString();
                    }
                    dic[propertyName] = array;
                }
            }
            return dic;
        }

        private static Dictionary<string, Dictionary<string, string[]>> ToDictionaryOfDictionaryOfStringArray(BlittableJsonReaderObject json, string name)
        {
            var dic = new Dictionary<string, Dictionary<string, string[]>>(StringComparer.OrdinalIgnoreCase);

            BlittableJsonReaderObject obj;
            //should a "null" exist in json? -> not sure that "null" can exist there
            if (json.TryGet(name, out obj) == false || obj == null)
                return dic;

            foreach (var propertyName in obj.GetPropertyNames())
            {
                BlittableJsonReaderObject result;
                if (obj.TryGet(propertyName, out result))
                {
                    var prop = new Dictionary<string, string[]>();
                    dic[propertyName] = prop;
                    foreach (var innerPropName in result.GetPropertyNames())
                    {
                        BlittableJsonReaderArray val;
                        if (result.TryGet(innerPropName, out val))
                        {
                            var array = new string[val.Length];
                            for (int i = 0; i < val.Length; i++)
                            {
                                array[i] = val[i]?.ToString();
                            }
                            prop[innerPropName] = array;
                        }
                    }

                }

            }
            return dic;
        }

        private static TCollection ToCollectionOfString<TCollection>(BlittableJsonReaderObject json, string name)
            where TCollection : ICollection<string>, new()
        {
            var collection = new TCollection();

            BlittableJsonReaderArray jsonArray;
            if (json.TryGet(name, out jsonArray) == false || jsonArray == null)
                return collection;

            foreach (var value in jsonArray)
                collection.Add(value.ToString());

            return collection;
        }

        private static string[] ToArrayOfString(BlittableJsonReaderObject json, string name)
        {
            var collection = new List<string>();

            BlittableJsonReaderArray jsonArray;
            if (json.TryGet(name, out jsonArray) == false || jsonArray == null)
                return collection.ToArray();

            foreach (var value in jsonArray)
                collection.Add(value.ToString());

            return collection.ToArray();
        }


        private static T ToObject<T>(BlittableJsonReaderObject json, string name, Func<BlittableJsonReaderObject, T> converter) where T : new()
        {
            BlittableJsonReaderObject obj;
            if (json.TryGet(name, out obj) == false || obj == null)
                return default(T);

            return converter(obj);
        }

        private static List<T> ToList<T>(BlittableJsonReaderObject json, string name, Func<BlittableJsonReaderObject, T> converter)
        {
            var list = new List<T>();

            BlittableJsonReaderArray array;
            if (json.TryGet(name, out array) == false)
                return list;

            foreach (BlittableJsonReaderObject item in array.Items)
                list.Add(converter(item));

            return list;
        }

        private static T[] ToArray<T>(BlittableJsonReaderObject json, string name, Func<BlittableJsonReaderObject, T> converter)
        {
            var list = new List<T>();

            BlittableJsonReaderArray array;
            if (json.TryGet(name, out array) == false)
                return list.ToArray();

            foreach (BlittableJsonReaderObject item in array.Items)
                list.Add(converter(item));

            return list.ToArray();
        }
    }
}