using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Raven.Abstractions.Data;
using Raven.Server.Documents.SqlReplication;
using Sparrow.Json;

namespace Raven.Server.Json
{
    public static class JsonDeserialization
    {
        public static readonly Func<BlittableJsonReaderObject, DatabaseDocument> DatabaseDocument = GenerateJsonDeserializationRoutine<DatabaseDocument>();

        public static readonly Func<BlittableJsonReaderObject, SqlReplicationConfiguration> SqlReplicationConfiguration = GenerateJsonDeserializationRoutine<SqlReplicationConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, SimulateSqlReplication> SimulateSqlReplication = GenerateJsonDeserializationRoutine<SimulateSqlReplication>();

        public static readonly Func<BlittableJsonReaderObject, PredefinedSqlConnections> PredefinedSqlConnections = GenerateJsonDeserializationRoutine<PredefinedSqlConnections>();

        public static readonly Func<BlittableJsonReaderObject, SqlReplicationTable> SqlReplicationTable = GenerateJsonDeserializationRoutine<SqlReplicationTable>();

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
                return Expression.Call(typeof (JsonDeserialization).GetMethod(nameof(ToDictionary)), json, Expression.Constant(propertyInfo.Name));
            }
            if (propertyInfo.PropertyType == typeof(List<SqlReplicationTable>))
            {
                return Expression.Call(typeof(JsonDeserialization).GetMethod(nameof(ToListSqlReplicationTable)), json, Expression.Constant(propertyInfo.Name));
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
        }
    }
}