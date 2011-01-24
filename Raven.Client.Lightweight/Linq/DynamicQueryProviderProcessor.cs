//-----------------------------------------------------------------------
// <copyright file="DynamicQueryProviderProcessor.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq.Expressions;
using Raven.Client.Document;
using Raven.Database.Data;

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
			string indexName) 
			: base(queryGenerator, customizeQuery, afterQueryExecuted, indexName)
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

			var memberExpression = GetMemberExpression(expression);

			//for stnadard queries, we take just the last part. Bu for dynamic queries, we take the whole part
			var path = memberExpression.ToString();
			path = path.Substring(path.IndexOf('.') + 1);


			var info = new ExpressionInfo(path, memberExpression.Member.GetMemberType(), memberExpression.Expression is MemberExpression);

			return new ExpressionInfo(
				CurrentPath + info.Path,
				info.Type,
				memberExpression.Expression is MemberExpression);
		}
 
		
	
	}
}
