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
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Json;
using ExpressionType = System.Linq.Expressions.ExpressionType;

namespace Raven.Server.Extensions
{
    ///<summary>
    /// Extensions for Linq expressions
    ///</summary>
    public static class ExpressionExtensions
    {
        public static void ThrowIfInvalidMethodInvocationInWhere(this QueryExpression where, BlittableJsonReaderObject parameters, string queryText, string whereCollectionName = null)
        {
            if (where is MethodExpression me)
            {
                var methodType = QueryMethod.GetMethodType(me.Name.Value);
                switch (methodType)
                {
                    case MethodType.Id:
                    case MethodType.CompareExchange:
                    case MethodType.Count:
                    case MethodType.Sum:
                    case MethodType.Spatial_Point:
                    case MethodType.Spatial_Wkt:
                    case MethodType.Spatial_Circle:
                        ThrowInvalidMethod(parameters, me, queryText, whereCollectionName);
                        break;
                }
            }
        }

        private static void ThrowInvalidMethod(BlittableJsonReaderObject parameters, MethodExpression me, string queryText, string whereCollectionName = null)
        {
            if (whereCollectionName == null)
            {
                throw new InvalidQueryException("A 'where' clause cannot contain just an '" + me.Name + "' method", queryText, parameters);
            }
            else
            {
                throw new InvalidQueryException($"A 'where' clause after '{whereCollectionName}' cannot contain just an '" + me.Name + "' method", queryText, parameters);
            }
        }

        public static Type ExtractTypeFromPath<T>(this Expression<Func<T, object>> path)
        {
            const char propertySeparator = '.';
            const char collectionSeparator = ',';
            var collectionSeparatorAsString = collectionSeparator.ToString();
            var propertyPath = path.ToPropertyPath(propertySeparator, collectionSeparator);
            var properties = propertyPath.Split(propertySeparator);
            var type = typeof(T);
            foreach (var property in properties)
            {
                if (property.Contains(collectionSeparatorAsString))
                {
                    var normalizedProperty = property.Replace(collectionSeparatorAsString, string.Empty);

                    if (type.IsArray)
                    {
                        type = type.GetElementType().GetProperty(normalizedProperty).PropertyType;
                    }
                    else
                    {
                        type = type.GetGenericArguments()[0].GetProperty(normalizedProperty).PropertyType;
                    }
                }
                else
                {
                    type = type.GetProperty(property).PropertyType;
                }
            }

            return type;
        }

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

        ///<summary>
        /// Turn an expression like x=&lt; x.User.Name to "User.Name"
        ///</summary>
        public static string ToPropertyPath(this LambdaExpression expr,
            char propertySeparator = '.',
            char collectionSeparator = ',')
        {
            var expression = expr.Body;

            return expression.ToPropertyPath(propertySeparator, collectionSeparator);
        }

        public static string ToPropertyPath(this Expression expression, char propertySeparator = '.', char collectionSeparator = ',')
        {
            var propertyPathExpressionVisitor = new PropertyPathExpressionVisitor(propertySeparator.ToString(), collectionSeparator.ToString());

            propertyPathExpressionVisitor.Visit(expression);

            var builder = new StringBuilder();

            var stackLength = propertyPathExpressionVisitor.Results.Count;

            for (var i = 0; i < stackLength; i++)
            {
                var curValue = propertyPathExpressionVisitor.Results.Pop();

                if (curValue.Length == 1 && curValue[0] == propertySeparator && i != stackLength - 1)
                {
                    var nextVal = propertyPathExpressionVisitor.Results.Peek();

                    if (nextVal.Length == 1 && nextVal[0] == collectionSeparator)
                    {
                        continue;
                    }
                }

                builder.Append(curValue);

            }
            return builder.ToString().Trim(propertySeparator, collectionSeparator);
        }

        public class PropertyPathExpressionVisitor : ExpressionVisitor
        {
            private readonly string _propertySeparator;
            private readonly string _collectionSeparator;
            public Stack<string> Results = new Stack<string>();

            public PropertyPathExpressionVisitor(string propertySeparator, string collectionSeparator)
            {
                _propertySeparator = propertySeparator;
                _collectionSeparator = collectionSeparator;
            }

            protected override Expression VisitMember(MemberExpression node)
            {
                if (IsDictionaryProperty(node, out string propertyName))
                {
                    if (string.IsNullOrEmpty(propertyName) == false)
                    {
                        Results.Push(_propertySeparator);
                        Results.Push("$" + node.Member.Name);
                    }

                    return base.VisitMember(node);
                }

                Results.Push(_propertySeparator);
                Results.Push(node.Member.Name);
                return base.VisitMember(node);
            }

            private static bool IsDictionaryProperty(MemberExpression node, out string propertyName)
            {
                propertyName = null;

                if (node.Member.DeclaringType == null)
                    return false;
                if (node.Member.DeclaringType.GetTypeInfo().IsGenericType == false)
                    return false;

                var genericTypeDefinition = node.Member.DeclaringType.GetGenericTypeDefinition();
                if (node.Member.Name == "Value" || node.Member.Name == "Key")
                {
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
    }
}
