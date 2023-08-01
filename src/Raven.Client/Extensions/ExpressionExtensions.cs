//-----------------------------------------------------------------------
// <copyright file="ExpressionExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
#if !RVN
using Raven.Client.Documents.Conventions;
#endif

namespace Raven.Client.Extensions
{
    ///<summary>
    /// Extensions for Linq expressions
    ///</summary>
    internal static class ExpressionExtensions
    {
        public static MemberInfo ToProperty(this LambdaExpression expr)
        {
            var expression = expr.Body;

            var unaryExpression = expression as UnaryExpression;
            if (unaryExpression != null)
            {
                switch (unaryExpression.NodeType)
                {
                    case ExpressionType.Convert:
                    case ExpressionType.ConvertChecked:
                        expression = unaryExpression.Operand;
                        break;
                }

            }

            var me = expression as MemberExpression;

            if (me == null)
                throw new InvalidOperationException("No idea how to convert " + expr.Body.NodeType + ", " + expr.Body +
                                                    " to a member expression");

            return me.Member;
        }

#if !RVN
        ///<summary>
        /// Turn an expression like x=&lt; x.User.Name to "User.Name"
        ///</summary>
        public static string ToPropertyPath(this LambdaExpression expr,
            DocumentConventions conventions,
            char propertySeparator = '.',
            string collectionSeparator = "[].")
        {
            var expression = expr.Body;

            return expression.ToPropertyPath(conventions, propertySeparator, collectionSeparator);
        }
        

        public static string ToPropertyPath(this Expression expression, DocumentConventions conventions, char propertySeparator = '.', string collectionSeparator = "[].")
        {
            var propertyPathExpressionVisitor = new PropertyPathExpressionVisitor(conventions, propertySeparator.ToString(), collectionSeparator);
            propertyPathExpressionVisitor.Visit(expression);

            var builder = new StringBuilder();

            var stackLength = propertyPathExpressionVisitor.Results.Count;

            for (var i = 0; i < stackLength; i++)
            {
                var curValue = propertyPathExpressionVisitor.Results.Pop();

                if (curValue.Length == 1 && curValue[0] == propertySeparator && i != stackLength - 1)
                {
                    var nextVal = propertyPathExpressionVisitor.Results.Peek();
                    if (nextVal.Length == collectionSeparator.Length && nextVal.Equals(collectionSeparator, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }
                }

                if (curValue == "$Value" && i != stackLength - 1)
                {
                    // Dictionary[].$Value.PropertyName => Dictionary[].PropertyName
                    if (builder.Length > 0 && builder[builder.Length - 1] == '.')
                        builder.Length--; // Dictionary[]..PropertyName => Dictionary[].PropertyName 
                    continue;
                }

                builder.Append(curValue);
            }

            return builder.ToString().Trim(propertySeparator, collectionSeparator[0], collectionSeparator[1], collectionSeparator[2]);
        }

        private sealed class PropertyPathExpressionVisitor : ExpressionVisitor
        {
            private readonly string _propertySeparator;
            private readonly string _collectionSeparator;
            public Stack<string> Results = new Stack<string>();
            private bool _isFirst = true;
            private readonly DocumentConventions _conventions;

            public PropertyPathExpressionVisitor(DocumentConventions conventions, string propertySeparator, string collectionSeparator)
            {
                _propertySeparator = propertySeparator;
                _collectionSeparator = collectionSeparator;
                _conventions = conventions;
            }

            protected override Expression VisitMember(MemberExpression node)
            {
                string convertedName = _conventions.GetConvertedPropertyNameFor(node.Member);
                if (IsDictionaryProperty(node, out string propertyName))
                {
                    if (string.IsNullOrEmpty(propertyName) == false)
                    {
                        AddPropertySeparator();
                        Results.Push("$" + convertedName);
                    }

                    return base.VisitMember(node);
                }

                if (_isFirst == false && node.Expression != null && node.Member.IsField())
                {
                    Results.Push(convertedName);
                    AddPropertySeparator();
                    Results.Push(node.Expression.ToString());
                    return base.VisitMember(node);
                }

                AddPropertySeparator();
                Results.Push(convertedName);
                return base.VisitMember(node);
            }

            private void AddPropertySeparator()
            {
                if (_isFirst)
                {
                    _isFirst = false;
                    return;
                }

                Results.Push(_propertySeparator);
            }

            private static bool IsDictionaryProperty(MemberExpression node, out string propertyName)
            {
                propertyName = null;

                if (node.Member.DeclaringType == null)
                    return false;
                if (node.Member.DeclaringType.IsGenericType == false)
                    return false;

                var genericTypeDefinition = node.Member.DeclaringType.GetGenericTypeDefinition();
                if (node.Member.Name == "Value" || node.Member.Name == "Key")
                {
                    propertyName = node.Member.Name;
                    return genericTypeDefinition == typeof(KeyValuePair<,>);
                }

                if (node.Member.Name == "Values" || node.Member.Name == "Keys")
                {
                    propertyName = node.Member.Name;
                    return genericTypeDefinition == typeof(Dictionary<,>) ||
                           genericTypeDefinition == typeof(IDictionary<,>);

                }

                return false;
            }

            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                if (node.Method.Name != "Select" && node.Arguments.Count != 2)
                    throw new InvalidOperationException("No idea how to deal with convert " + node + " to a member expression");


                Visit(node.Arguments[1]);
                Results.Push(_collectionSeparator);
                Visit(node.Arguments[0]);


                return node;
            }
        }
#endif
    }
}
