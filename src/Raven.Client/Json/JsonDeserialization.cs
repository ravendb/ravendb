using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using Raven.Abstractions.Data;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Json
{
    public class JsonDeserialization
    {
        private static readonly Type[] EmptyTypes = new Type[0];

        public static readonly Func<BlittableJsonReaderObject, PutResult> PutResult = GenerateJsonDeserializationRoutine<PutResult>();
        public static readonly Func<BlittableJsonReaderObject, AuthenticatorChallenge> AuthenticatorChallenge = GenerateJsonDeserializationRoutine<AuthenticatorChallenge>();
        public static readonly Func<BlittableJsonReaderObject, Topology> ClusterTopology = GenerateJsonDeserializationRoutine<Topology>();

        public static readonly Func<BlittableJsonReaderObject, TcpConnectionHeaderMessage> TcpConnectionHeaderMessage = GenerateJsonDeserializationRoutine<TcpConnectionHeaderMessage>();

        public static readonly Func<BlittableJsonReaderObject, DatabaseDocument> DatabaseDocument = GenerateJsonDeserializationRoutine<DatabaseDocument>();

        public static Func<BlittableJsonReaderObject, T> GenerateJsonDeserializationRoutine<T>()
        {
            try
            {
                var json = Expression.Parameter(typeof(BlittableJsonReaderObject), "json");

                var vars = new Dictionary<Type, ParameterExpression>();
                var instance = Expression.New(typeof(T).GetConstructor(EmptyTypes));
                var propInit = new List<MemberBinding>();
                foreach (var fieldInfo in typeof(T).GetFields())
                {
                    propInit.Add(Expression.Bind(fieldInfo, GetValue(fieldInfo.Name, fieldInfo.FieldType, json, vars)));
                }
                foreach (var propertyInfo in typeof(T).GetProperties())
                {
                    if (propertyInfo.CanWrite == false)
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

        //TODO : consider refactoring JsonDeserialization::GetValue() to be more generic
        //since this is understandble and clear code while it is short,
        //when it will become longer, it is likely to cause issues
        private static Expression GetValue(string propertyName, Type propertyType, ParameterExpression json, Dictionary<Type, ParameterExpression> vars)
        {
            var type = Nullable.GetUnderlyingType(propertyType) ?? propertyType;
            if (type == typeof(string) ||
                type == typeof(bool) ||
                type == typeof(long) ||
                type == typeof(int) ||
                type == typeof(double) ||
                type.GetTypeInfo().IsEnum ||
                type == typeof(DateTime))
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

            if (propertyType.Name == "Dictionary`2")
            {
                var valueType = propertyType.GenericTypeArguments[1];
                if (valueType == typeof(string))
                {
                    var methodToCall = typeof(JsonDeserialization).GetMethod(nameof(ToDictionaryOfString));
                    return Expression.Call(methodToCall, json, Expression.Constant(propertyName));
                }
                else
                {
                    // TODO: Do not duplicate, use the same as #2
                    var convert = typeof(JsonDeserialization).GetMethod(nameof(GenerateJsonDeserializationRoutine)).MakeGenericMethod(valueType)
                        .Invoke(null, null);

                    var constantExpression = Expression.Constant(convert);
                    var methodToCall = typeof(JsonDeserialization).GetMethod(nameof(ToDictionary)).MakeGenericMethod(valueType);
                    return Expression.Call(methodToCall, json, Expression.Constant(propertyName), constantExpression);
                }
            }

            /*if (propertyInfo.PropertyType == typeof(List<SqlReplicationTable>))
            {
                return Expression.Call(typeof(JsonDeserialization).GetMethod(nameof(ToListSqlReplicationTable)), json, Expression.Constant(propertyInfo.Name));
            }

            if (propertyInfo.PropertyType == typeof(List<ReplicationDestination>))
            {
                return Expression.Call(typeof(JsonDeserialization).GetMethod(nameof(ToListReplicationDestination)), json, Expression.Constant(propertyInfo.Name));
            }*/

            if (propertyType == typeof(HashSet<string>))
            {
                return Expression.Call(typeof(JsonDeserialization).GetMethod(nameof(ToHashSetOfString)), json, Expression.Constant(propertyName));
            }

            // TODO: Do not duplicate, use the same as #1
            var converterField = typeof(JsonDeserialization).GetField(propertyType.Name, BindingFlags.Static | BindingFlags.Public);
            if (converterField != null)
            {
                var converter = (Delegate)converterField.GetValue(null);
                if (converter == null)
                    throw new InvalidOperationException($"{propertyType.Name} field is not initialized yet.");
                var methodToCall = typeof(JsonDeserialization).GetMethod(nameof(ToObject)).MakeGenericMethod(propertyType);
                var constantExpression = Expression.Constant(converter);
                return Expression.Call(methodToCall, json, Expression.Constant(propertyName), constantExpression);
            }

            throw new InvalidOperationException($"We weren't able to convert the property '{propertyName}' of type '{type}'.");
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

        public static Dictionary<string, T> ToDictionary<T>(BlittableJsonReaderObject json, string name, Func<BlittableJsonReaderObject, T> converter)
        {
            var dic = new Dictionary<string, T>(StringComparer.OrdinalIgnoreCase);

            BlittableJsonReaderObject obj;
            if (json.TryGet(name, out obj) == false)
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

        public static Dictionary<string, string> ToDictionaryOfString(BlittableJsonReaderObject json, string name)
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

        public static HashSet<string> ToHashSetOfString(BlittableJsonReaderObject json, string name)
        {
            var hashSet = new HashSet<string>();

            BlittableJsonReaderArray jsonArray;
            if (json.TryGet(name, out jsonArray) == false || jsonArray == null)
                return hashSet;

            foreach (var value in jsonArray)
                hashSet.Add(value.ToString());

            return hashSet;
        }

        public static T ToObject<T>(BlittableJsonReaderObject json, string name, Func<BlittableJsonReaderObject, T> converter) where T : new()
        {
            BlittableJsonReaderObject obj;
            if (json.TryGet(name, out obj) == false || obj == null)
                return default(T);

            return converter(obj);
        }

        /*public static List<ReplicationDestination> ToListReplicationDestination(BlittableJsonReaderObject json, string name)
        {
            var list = new List<ReplicationDestination>();

            BlittableJsonReaderArray array;
            if (json.TryGet(name, out array) == false)
                return list;

            foreach (BlittableJsonReaderObject item in array.Items)
                list.Add(ReplicationDestination(item));

            return list;
        }

        public static List<SqlReplicationTable> ToListSqlReplicationTable(BlittableJsonReaderObject json, string name)
        {
            var list = new List<SqlReplicationTable>();

            BlittableJsonReaderArray array;
            if (json.TryGet(name, out array) == false)
                return list;

            foreach (BlittableJsonReaderObject item in array.Items)
            {
                list.Add(SqlReplicationTable(item));
            }
            return list;
        }*/
    }
}