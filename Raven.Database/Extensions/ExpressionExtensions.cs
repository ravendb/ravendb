using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Raven.Database.Extensions 
{
    ///<summary>
    /// Extensions for Linq expressions
    ///</summary>
    public static class ExpressionExtensions 
    {
        ///<summary>
        /// Turn an expression like x=&lt; x.User.Name to "User.Name"
        ///</summary>
        ///<param name="expr">Expression for member access</param>
        public static string ToPropertyPath<T, TProperty>(this Expression<Func<T, TProperty>> expr)
        {
            var me = expr.Body as MemberExpression;
            var parts = new List<string>();
            while (me != null)
            {
                parts.Insert(0, me.Member.Name);
                me = me.Expression as MemberExpression;
            }
            return String.Join(".", parts.ToArray());
        }
    }
}