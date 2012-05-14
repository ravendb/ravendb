using System;
using System.Linq.Expressions;

namespace Raven.Client.Linq
{
	internal static class ExpressionExtensions
	{
		public static string GetPropertyName<T, TValue>(this Expression<Func<T, TValue>> propertySelector)
		{
			var memberExpression = propertySelector.Body as MemberExpression;
			if (memberExpression != null)
			{
				return memberExpression.Member.Name;
			}
			throw new ArgumentException("Can not resolve property name");
		}
	}
}