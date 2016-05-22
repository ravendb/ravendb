using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.SqlReplication;
using Sparrow.Json;

namespace Raven.Server.Json
{
    public static class JsonDeserialization
    {		
        public static readonly Func<BlittableJsonReaderObject, ReplicationDocument> ReplicationDocument = GenerateJsonDeserializationRoutine<ReplicationDocument>();

        public static readonly Func<BlittableJsonReaderObject, ReplicationClientConfiguration> ReplicationClientConfiguration = GenerateJsonDeserializationRoutine<ReplicationClientConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, ReplicationDestination> ReplicationDestination = GenerateJsonDeserializationRoutine<ReplicationDestination>();

        public static readonly Func<BlittableJsonReaderObject, DatabaseDocument> DatabaseDocument = GenerateJsonDeserializationRoutine<DatabaseDocument>();

        public static readonly Func<BlittableJsonReaderObject, SqlReplicationConfiguration> SqlReplicationConfiguration = GenerateJsonDeserializationRoutine<SqlReplicationConfiguration>();

        public static readonly Func<BlittableJsonReaderObject, SimulateSqlReplication> SimulateSqlReplication = GenerateJsonDeserializationRoutine<SimulateSqlReplication>();

        public static readonly Func<BlittableJsonReaderObject, PredefinedSqlConnection> PredefinedSqlConnection = GenerateJsonDeserializationRoutine<PredefinedSqlConnection>();

        public static readonly Func<BlittableJsonReaderObject, SqlReplicationTable> SqlReplicationTable = GenerateJsonDeserializationRoutine<SqlReplicationTable>();

        public static readonly Func<BlittableJsonReaderObject, SqlReplicationStatus> SqlReplicationStatus = GenerateJsonDeserializationRoutine<SqlReplicationStatus>();

        public static readonly Func<BlittableJsonReaderObject, SubscriptionCriteria> SubscriptionCriteria = GenerateJsonDeserializationRoutine<SubscriptionCriteria>();

        public static readonly Func<BlittableJsonReaderObject, SubscriptionConnectionOptions> SubscriptionCriteriaOptions = GenerateJsonDeserializationRoutine<SubscriptionConnectionOptions>();


        
        public static Func<BlittableJsonReaderObject,T> GenerateJsonDeserializationRoutine<T>()
        {
            try
            {
                var json = Expression.Parameter(typeof(BlittableJsonReaderObject), "json");

                var vars = new Dictionary<Type, ParameterExpression>();
                var instance = Expression.New(typeof(T).GetConstructor(new Type[0]));
                var propInit = new List<MemberBinding>();
                foreach (var propertyInfo in typeof(T).GetProperties())
                {
                    if (propertyInfo.CanWrite == false)
                        continue;
                    propInit.Add(Expression.Bind(propertyInfo, GetValue(propertyInfo, json, vars)));
                }

                var lambda = Expression.Lambda<Func<BlittableJsonReaderObject, T>>(Expression.Block(vars.Values, Expression.MemberInit(instance, propInit)), json);

                return lambda.Compile();
            }
            catch (Exception e)
            {
                return o =>
                {
                    throw new InvalidOperationException($"Could not build json parser for {typeof (T).FullName}", e);
                };
            }
        }

        //TODO : consider refactoring JsonDeserialization::GetValue() to be more generic
        //since this is understandble and clear code while it is short,
        //when it will become longer, it is likely to cause issues
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

            if (propertyInfo.PropertyType == typeof(List<ReplicationDestination>))
            {
                return Expression.Call(typeof(JsonDeserialization).GetMethod(nameof(ToListReplicationDestination)), json, Expression.Constant(propertyInfo.Name));
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

        public static List<ReplicationDestination> ToListReplicationDestination(BlittableJsonReaderObject json, string name)
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
        }
    }
}