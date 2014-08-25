//-----------------------------------------------------------------------
// <copyright file="CaptureSelectNewFieldNamesVisitor.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel.Security.Tokens;
using ICSharpCode.NRefactory.CSharp;

namespace Raven.Database.Linq.Ast
{
	[CLSCompliant(false)]
	public class CaptureSelectNewFieldNamesVisitor : DepthFirstAstVisitor<object, object>
	{
		private readonly bool _outerMostRequired;
		private HashSet<string> fieldNames;
		private Dictionary<string,Expression> selectExpressions;
		private bool queryProcessed;

		public CaptureSelectNewFieldNamesVisitor(bool outerMostRequired, HashSet<string> fieldNames, Dictionary<string, Expression> selectExpressions)
		{
			_outerMostRequired = outerMostRequired;
			this.fieldNames = fieldNames;
			this.selectExpressions = selectExpressions;
		}

		public HashSet<string> FieldNames { get{return fieldNames;} }

		public override object VisitQuerySelectClause(QuerySelectClause querySelectClause, object data)
		{
			ProcessQuery(querySelectClause.Expression);
			return base.VisitQuerySelectClause(querySelectClause, data);
		}


		public void Clear()
		{
			queryProcessed = false;
			fieldNames.Clear();
            selectExpressions.Clear();
		}


		public void ProcessQuery(AstNode queryExpressionSelectClause)
		{
			var objectCreateExpression = QueryParsingUtils.GetAnonymousCreateExpression(queryExpressionSelectClause) as AnonymousTypeCreateExpression;
			if (objectCreateExpression == null)
				return;

			// we only want the outer most value
			if (queryProcessed && _outerMostRequired)
				return;

			fieldNames.Clear();
			selectExpressions.Clear();

			queryProcessed = true;

            foreach (var expression in objectCreateExpression.Initializers.OfType<NamedArgumentExpression>())
            {
                fieldNames.Add(expression.Name);
                selectExpressions[expression.Name] = expression.Expression;
            }

		    foreach (var expression in objectCreateExpression.Initializers.OfType<NamedExpression>())
		    {
		        fieldNames.Add(expression.Name);
		        selectExpressions[expression.Name] = expression.Expression;

		    }
		    foreach (var expression in objectCreateExpression.Initializers.OfType<MemberReferenceExpression>())
			{
				fieldNames.Add(expression.MemberName);
			    selectExpressions[expression.MemberName] = expression;
			}

			foreach (var expression in objectCreateExpression.Initializers.OfType<IdentifierExpression>())
			{
				fieldNames.Add(expression.Identifier);
			    selectExpressions[expression.Identifier] = expression;
			}
		}

		public override object VisitInvocationExpression(InvocationExpression invocationExpression, object data)
		{
			var memberReferenceExpression = invocationExpression.Target as MemberReferenceExpression;

			if (memberReferenceExpression == null)
				return base.VisitInvocationExpression(invocationExpression, data);

			LambdaExpression lambdaExpression;
			switch (memberReferenceExpression.MemberName)
			{
                case "Select":
                    if (invocationExpression.Arguments.Count != 1)
                        return base.VisitInvocationExpression(invocationExpression, data);
                    lambdaExpression = invocationExpression.Arguments.First().AsLambdaExpression();
                    break;
                case "SelectMany":
					if (invocationExpression.Arguments.Count != 2)
						return base.VisitInvocationExpression(invocationExpression, data);
					lambdaExpression = invocationExpression.Arguments.ElementAt(1).AsLambdaExpression();
					break;
				default:
					return base.VisitInvocationExpression(invocationExpression, data);
			}

			if (lambdaExpression == null)
				return base.VisitInvocationExpression(invocationExpression, data);

			ProcessQuery(lambdaExpression.Body);

			return base.VisitInvocationExpression(invocationExpression, data);
		}
	}
}