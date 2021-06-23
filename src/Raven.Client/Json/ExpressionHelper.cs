using System;
using System.Collections.Generic;
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

        public static Action<TClass> SafelyClearList<TClass>(string fieldName)
        {
            var field = typeof(TClass).GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Instance);
            var clearMethod = field.FieldType.GetMethod(nameof(List<object>.Clear));

            var targetExp = Expression.Parameter(typeof(TClass), "target");
            var fieldExp = Expression.Field(targetExp.CastFromObject(field.DeclaringType), field);

            var notEqualExp = Expression.NotEqual(fieldExp, Expression.Constant(null, field.FieldType));
            var callExp = Expression.Call(fieldExp, clearMethod);

            var conditionExp = Expression.Condition(notEqualExp, callExp, Expression.Empty());

            return Expression.Lambda<Action<TClass>>(conditionExp, targetExp).Compile();
        }

        private static Expression CastFromObject(this Expression expr, Type targetType)
        {
            return expr.Type == targetType ? expr :
              targetType.GetTypeInfo().IsClass ? Expression.TypeAs(expr, targetType) :
              targetType.GetTypeInfo().IsValueType ? Expression.Unbox(expr, targetType) : Expression.Convert(expr, targetType);
        }
    }
}
