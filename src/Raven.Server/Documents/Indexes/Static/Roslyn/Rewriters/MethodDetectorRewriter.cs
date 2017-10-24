using Lucene.Net.Documents;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public class MethodDetectorRewriter : CSharpSyntaxRewriter
    {
        public readonly IndexCompiler.IndexMethods Methods = new IndexCompiler.IndexMethods();

        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            var expression = node.Expression.ToString();
            switch (expression)
            {
                case "this.LoadDocument":
                case "LoadDocument":
                    Methods.HasLoadDocument = true;
                    break;
                case "this.TransformWith":
                case "TransformWith":
                    Methods.HasTransformWith = true;
                    break;
                case "this.Include":
                case "Include":
                    Methods.HasInclude = true;
                    break;
                case "results.GroupBy":
                    Methods.HasGroupBy = true;
                    break;
                case "this.CreateField":
                case "CreateField":
                    Methods.HasCreateField = true;
                    break;
            }

            var memberAccessExpression = node.Expression as MemberAccessExpressionSyntax;

            if (memberAccessExpression != null)
            {
                switch (memberAccessExpression.Name.Identifier.Text)
                {
                    case "Boost":
                        Methods.HasBoost = true;
                        break;
                }
            }

            return base.VisitInvocationExpression(node);
        }

        public override SyntaxNode VisitObjectCreationExpression(ObjectCreationExpressionSyntax node)
        {
            var type = node.Type.ToString();
            if (type == nameof(Field) || type == nameof(NumericField) || type == typeof(Field).FullName || type == typeof(NumericField).FullName)
                Methods.HasCreateField = true;

            return base.VisitObjectCreationExpression(node);
        }

        public override SyntaxNode VisitGroupClause(GroupClauseSyntax node)
        {
            Methods.HasGroupBy = true;
            return base.VisitGroupClause(node);
        }
    }
}
