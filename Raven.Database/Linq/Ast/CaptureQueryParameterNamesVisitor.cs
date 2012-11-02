using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ICSharpCode.NRefactory.CSharp;

namespace Raven.Database.Linq.Ast
{
	[CLSCompliant(false)]
	public class CaptureQueryParameterNamesVisitor : DepthFirstAstVisitor<object, object>
	{
		private readonly HashSet<string> queryParameters = new HashSet<string>();

		private readonly Dictionary<string, string> aliasToName = new Dictionary<string, string>();

		public HashSet<string> QueryParameters
		{
			get { return queryParameters; }
		}
		
		public override object VisitQueryFromClause(QueryFromClause queryFromClause, object data)
		{
			var memberReferenceExpression = queryFromClause.Expression as MemberReferenceExpression;
			if (memberReferenceExpression != null)
			{
				var identifierExpression = memberReferenceExpression.Target as IdentifierExpression;
				if (identifierExpression != null && identifierExpression.Identifier != "docs")
				{
					var generateExpression = GenerateExpression(queryFromClause.Expression);
					if (generateExpression != null)
						aliasToName[queryFromClause.Identifier] = generateExpression + ",";
				}
			}

			return base.VisitQueryFromClause(queryFromClause, data);
		}

		private void ProcessQuery(Expression queryExpressionSelectClause)
		{
			var objectCreateExpression = QueryParsingUtils.GetAnonymousCreateExpression(queryExpressionSelectClause) as AnonymousTypeCreateExpression;
			if (objectCreateExpression == null)
				return;

			foreach (var expression in objectCreateExpression.Initializers.OfType<NamedArgumentExpression>())
			{
				var generateExpression = GenerateExpression(expression.Expression);
				if (generateExpression != null)
					QueryParameters.Add(generateExpression);
			}


			foreach (var expression in objectCreateExpression.Initializers.OfType<NamedExpression>())
			{
				var generateExpression = GenerateExpression(expression.Expression);
				if (generateExpression != null)
					QueryParameters.Add(generateExpression);
			}

			foreach (var expression in  objectCreateExpression.Initializers.OfType<MemberReferenceExpression>())
			{
				var generateExpression = GenerateExpression(expression);
				if (generateExpression != null)
					QueryParameters.Add(generateExpression);
			}
		}

		private string GenerateExpression(Expression expression)
		{
			var sb = new StringBuilder();
			var memberReferenceExpression = expression as MemberReferenceExpression;
			while (memberReferenceExpression != null)
			{
				if (sb.Length != 0)
					sb.Insert(0, ".");

				sb.Insert(0, memberReferenceExpression.MemberName);

				expression = memberReferenceExpression.Target;
				memberReferenceExpression = expression as MemberReferenceExpression;
			}

			var identifierExpression = expression as IdentifierExpression;
			if (identifierExpression != null && sb.Length != 0)
			{
				string path;
				if (aliasToName.TryGetValue(identifierExpression.Identifier, out path))
				{
					sb.Insert(0, path);
				}
			}
			if (sb.Length == 0)
				return null;

			return sb.ToString();
		}

		public override object VisitQuerySelectClause(QuerySelectClause querySelectClause, object data)
		{
			ProcessQuery(querySelectClause.Expression);
			return base.VisitQuerySelectClause(querySelectClause, data);
		}

		public override object VisitQueryLetClause(QueryLetClause queryExpressionLetClause, object data)
		{
			var generateExpression = GenerateExpression(queryExpressionLetClause.Expression);
			if (generateExpression != null)
				aliasToName[queryExpressionLetClause.Identifier] = generateExpression + ".";
			return base.VisitQueryLetClause(queryExpressionLetClause, data);
		}

	}
}