using System;
using System.Linq.Expressions;
using System.Reflection;
using Sparrow;

namespace Raven.Client.Json
{
    internal static class ExpressionHelper
    {
        public static Action<TClass, TField> CreateFieldSetter<TClass, TField>(string fieldName)
        {
            var field = typeof(TClass).GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Instance);
            var targetExp = Expression.Parameter(typeof(TClass), "target");
            var valueExp = Expression.Parameter(typeof(TField), "value");

            var fieldExp = Expression.Field(targetExp.CastFromObject(field.DeclaringType), field);
            var assignExp = Expression.Assign(fieldExp, valueExp.CastFromObject(field.FieldType));
            return Expression.Lambda<Action<TClass, TField>>(assignExp, targetExp, valueExp).Compile();
        }

        public static Action<TClass> CreateZeroFieldFunction<TClass>(string fieldName)
        {
            FieldInfo field = typeof(TClass).GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Instance);
            var param = Expression.Parameter(typeof(TClass), "param");
            var fieldExpression = Expression.Field(param, field);

            var body = Expression.Call(null, typeof(Sodium).GetMethod("ZeroBuffer"), fieldExpression);
            return Expression.Lambda<Action<TClass>>(body, param).Compile();
        }

        private static Expression CastFromObject(this Expression expr, Type targetType)
        {
            return expr.Type == targetType ? expr :
              targetType.GetTypeInfo().IsClass ? Expression.TypeAs(expr, targetType) :
              targetType.GetTypeInfo().IsValueType ? Expression.Unbox(expr, targetType) : Expression.Convert(expr, targetType);
        }
    }
}
