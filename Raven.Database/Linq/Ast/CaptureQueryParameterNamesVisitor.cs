using System.Collections.Generic;
using System.Linq;
using System.Text;
using ICSharpCode.NRefactory.Ast;
using ICSharpCode.NRefactory.Visitors;

namespace Raven.Database.Linq.Ast
{
	public class CaptureQueryParameterNamesVisitor : AbstractAstVisitor
	{
		private readonly HashSet<string> queryParameters = new HashSet<string>();

		private readonly Dictionary<string, string> aliasToName = new Dictionary<string, string>();

		public HashSet<string> QueryParameters
		{
			get { return queryParameters; }
		}

		public override object VisitQueryExpressionFromClause(QueryExpressionFromClause queryExpressionFromClause, object data)
		{
			var memberReferenceExpression = queryExpressionFromClause.InExpression as MemberReferenceExpression;
			if (memberReferenceExpression != null)
			{
				var identifierExpression = memberReferenceExpression.TargetObject as IdentifierExpression;
				if (identifierExpression != null && identifierExpression.Identifier != "docs")
				{
					var generateExpression = GenerateExpression(queryExpressionFromClause.InExpression);
					if (generateExpression != null)
						aliasToName[queryExpressionFromClause.Identifier] = generateExpression + ",";
				}
			}
			return base.VisitQueryExpressionFromClause(queryExpressionFromClause, data);
		}

		public override object VisitQueryExpressionSelectClause(QueryExpressionSelectClause queryExpressionSelectClause,
															   object data)
		{
			ProcessQuery(queryExpressionSelectClause.Projection);
			return base.VisitQueryExpressionSelectClause(queryExpressionSelectClause, data);
		}

		public override object VisitQueryExpressionLetClause(QueryExpressionLetClause queryExpressionLetClause, object data)
		{
			var generateExpression = GenerateExpression(queryExpressionLetClause.Expression);
			if (generateExpression != null)
				aliasToName[queryExpressionLetClause.Identifier] = generateExpression + ".";
			return base.VisitQueryExpressionLetClause(queryExpressionLetClause, data);
		}

		private void ProcessQuery(Expression queryExpressionSelectClause)
		{
			var objectCreateExpression = QueryParsingUtils.GetAnonymousCreateExpression(queryExpressionSelectClause) as ObjectCreateExpression;
			if (objectCreateExpression == null ||
				objectCreateExpression.IsAnonymousType == false)
				return;

			foreach (
				var expression in
					objectCreateExpression.ObjectInitializer.CreateExpressions.OfType<NamedArgumentExpression>())
			{
				var generateExpression = GenerateExpression(expression.Expression);
				if (generateExpression != null)
					QueryParameters.Add(generateExpression);
			}

			foreach (
				var expression in
					objectCreateExpression.ObjectInitializer.CreateExpressions.OfType<MemberReferenceExpression>())
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

				expression = memberReferenceExpression.TargetObject;
				memberReferenceExpression = expression as MemberReferenceExpression;
			}

			var identifierExpression = expression as IdentifierExpression;
			if(identifierExpression != null && sb.Length != 0)
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
	}
}