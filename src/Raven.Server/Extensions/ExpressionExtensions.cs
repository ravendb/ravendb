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
                throw new InvalidQueryException("A 'where' clause cannot contain just a '" + me.Name + "' method", queryText, parameters);
            }

            throw new InvalidQueryException($"A 'where' clause after '{whereCollectionName}' cannot contain just a '" + me.Name + "' method", queryText, parameters);
        }
    }
}
