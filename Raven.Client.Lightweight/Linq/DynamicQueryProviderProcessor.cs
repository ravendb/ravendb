//-----------------------------------------------------------------------
// <copyright file="DynamicQueryProviderProcessor.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Abstractions.Data;
using Raven.Client.Document;

namespace Raven.Client.Linq
{
	/// <summary>
	/// A specialized query provider processor for querying dynamic types
	/// </summary>
	public class DynamicQueryProviderProcessor<T> : RavenQueryProviderProcessor<T>
	{
		/// <summary>
		/// Creates a dynamic query provider around the provided session
		/// </summary>
		public DynamicQueryProviderProcessor(
			IDocumentQueryGenerator queryGenerator,
			Action<IDocumentQueryCustomization> customizeQuery, 
			Action<QueryResult> afterQueryExecuted,
			string indexName,
			HashSet<string> fieldsToFetch) 
			: base(queryGenerator, customizeQuery, afterQueryExecuted, indexName, fieldsToFetch)
		{

		}

		/// <summary>
		/// Gets member info for the specified expression and the path to that expression
		/// </summary>
		/// <param name="expression"></param>
		/// <returns></returns>
		protected override ExpressionInfo GetMember(System.Linq.Expressions.Expression expression)
		{
			var parameterExpression = expression as ParameterExpression;
			if (parameterExpression != null)
			{
				return new ExpressionInfo(CurrentPath, parameterExpression.Type, false);
			}

			string path;
			Type memberType;
			bool isNestedPath;
			GetPath(expression, out path, out memberType, out isNestedPath);

			//for standard queries, we take just the last part. But for dynamic queries, we take the whole part
			path = path.Substring(path.IndexOf('.') + 1);

			return new ExpressionInfo(
				queryGenerator.Conventions.FindPropertyNameForDynamicIndex(typeof(T), indexName, CurrentPath, path), 
				memberType,
				isNestedPath);
		}

		 
	}
}
