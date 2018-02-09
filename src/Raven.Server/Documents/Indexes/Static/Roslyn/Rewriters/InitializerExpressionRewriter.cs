using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public class InitializerExpressionRewriter : CSharpSyntaxRewriter
    {
        public static readonly InitializerExpressionRewriter Instance = new InitializerExpressionRewriter();

        private InitializerExpressionRewriter()
        {
        }

        public override SyntaxNode VisitInitializerExpression(InitializerExpressionSyntax node)
        {
            TypeSyntax typeToCast = null;
            var rankCount = 0;

            if (node.Parent is ArrayCreationExpressionSyntax array)
            {
                typeToCast = array.Type.ElementType;
                rankCount = array.Type.RankSpecifiers.Count - 1;
            }
            else if (node.IsKind(SyntaxKind.CollectionInitializerExpression) && node.Parent is ObjectCreationExpressionSyntax obj) // e.g. List<int> { document.Field }
            {
                var name = obj.Type as GenericNameSyntax;
                if (name == null && obj.Type is QualifiedNameSyntax qualifiedName)
                    name = qualifiedName.Right as GenericNameSyntax;

                if (name != null)
                {
                    var arguments = name.TypeArgumentList.Arguments;
                    if (arguments.Count == 1)
                        typeToCast = arguments[0];
                }
            }

            if (typeToCast != null)
            {
                var castedExpression = new SeparatedSyntaxList<ExpressionSyntax>();
                foreach (var expression in node.Expressions)
                {
                    if (expression is InitializerExpressionSyntax initializerExpression) // e.g. List<int> { document.Field }
                    {
                        var expressions = initializerExpression.Expressions;
                        if (expressions.Count == 1)
                        {
                            var innerExpression = expressions[0];

                            expressions = expressions.Replace(innerExpression, CastExpression(typeToCast, rankCount, innerExpression));
                            initializerExpression = initializerExpression.WithExpressions(expressions);
                        }

                        castedExpression = castedExpression.Add(initializerExpression);
                        continue;
                    }

                    castedExpression = castedExpression.Add(CastExpression(typeToCast, rankCount, expression));
                }

                return node.WithExpressions(castedExpression);
            }

            return node;
        }

        private static ExpressionSyntax CastExpression(TypeSyntax typeToCast, int rankCount, ExpressionSyntax expression)
        {
            string rank = null;
            for (var i = 0; i < rankCount; i++)
                rank += "[]";

            return SyntaxFactory.ParseExpression($"({typeToCast}{rank})({expression})");
        }
    }
}
