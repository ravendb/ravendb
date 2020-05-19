using System;
using System.Linq.Expressions;
using System.Reflection;

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

        private static Expression CastFromObject(this Expression expr, Type targetType)
        {
            return expr.Type == targetType ? expr :
              targetType.IsClass ? Expression.TypeAs(expr, targetType) :
              targetType.IsValueType ? Expression.Unbox(expr, targetType) : Expression.Convert(expr, targetType);
        }
    }
}
