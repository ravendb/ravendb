using System;
using System.Linq.Expressions;
using System.Reflection;
using JetBrains.Annotations;

namespace Lambda2Js
{
    static class PropertyInfoExtensions
    {
        public static Func<object, TProperty> MakeGetterDelegate<TProperty>([NotNull] this PropertyInfo property)
        {
            if (property == null)
                throw new ArgumentNullException(nameof(property));

            var getMethod = property.GetGetMethod();
            if (getMethod != null)
            {
                var target = Expression.Parameter(typeof(object));
                var body = Expression.Call(Expression.ConvertChecked(target, property.DeclaringType), getMethod);
                return Expression.Lambda<Func<object, TProperty>>(body, target).Compile();
            }

            return null;
        }

    }
}