using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;
using static Microsoft.CodeAnalysis.CSharp.SyntaxFactory;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public class RavenLinqPrettifier : CSharpSyntaxRewriter
    {
        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            var memeberAccess = node.Expression as MemberAccessExpressionSyntax;
            if (memeberAccess == null)
                return base.VisitInvocationExpression(node);

            if (memeberAccess.Name.Identifier.ValueText != "Select")
            {
                return base.VisitInvocationExpression(node);
            }
            var expressionSyntax = node.ArgumentList.Arguments[0].Expression as SimpleLambdaExpressionSyntax;
            if (expressionSyntax == null)
            {
                return base.VisitInvocationExpression(node);
            }
            return QueryExpression(
                FromClause(
                    expressionSyntax.Parameter.Identifier,
                    ParenthesizedExpression((ExpressionSyntax)Visit(memeberAccess.Expression))
                ),
                QueryBody(SelectClause((ExpressionSyntax)Visit(expressionSyntax.Body)))
            ).NormalizeWhitespace();

        }
    }

    public class RavenLinqOptimizer : CSharpSyntaxRewriter
    {

        public override SyntaxNode VisitQueryExpression(QueryExpressionSyntax node)
        {
            var stripExpressionParentParenthesis = StripExpressionParentParenthesis(node);
            if (stripExpressionParentParenthesis != null &&
                stripExpressionParentParenthesis is QueryExpressionSyntax == false)
                return node;

            ForEachStatementSyntax parent = null;
            var queryExpressionSyntax = StripExpressionParenthesis(node.FromClause.Expression) as QueryExpressionSyntax;
            if (queryExpressionSyntax != null &&
                StripExpressionParentParenthesis(queryExpressionSyntax) is QueryExpressionSyntax)
            {
                parent = VisitQueryExpression(queryExpressionSyntax) as ForEachStatementSyntax;

                if (parent != null)
                    node =
                        node.WithFromClause(
                            node.FromClause.WithExpression(IdentifierName(queryExpressionSyntax.FromClause.Identifier)));
            }

            var body = Block();
            ForEachStatementSyntax innerStmt = null;

            foreach (var clause in node.Body.Clauses)
            {
                var whereClauseSyntax = clause as WhereClauseSyntax;
                if (whereClauseSyntax != null)
                {
                    body = body.AddStatements(IfStatement(BinaryExpression(SyntaxKind.EqualsExpression, ParenthesizedExpression(whereClauseSyntax.Condition), LiteralExpression(SyntaxKind.FalseLiteralExpression)), ContinueStatement()));
                    continue;
                }
                var letClauseSyntax = clause as LetClauseSyntax;
                if (letClauseSyntax != null)
                {
                    body = body.AddStatements(
                        LocalDeclarationStatement(
                            VariableDeclaration(IdentifierName("var"), SingletonSeparatedList(VariableDeclarator(
                                        letClauseSyntax.Identifier)
                                    .WithInitializer(
                                        EqualsValueClause(letClauseSyntax.Expression)
                                    )
                                )
                            )
                        )
                    );
                    continue;
                }
                var fromClauseSyntax = clause as FromClauseSyntax;
                if (fromClauseSyntax == null)
                {
                    return base.VisitQueryExpression(node);
                }

                innerStmt = ForEachStatement(
                    IdentifierName("var"),
                    fromClauseSyntax.Identifier,
                    fromClauseSyntax.Expression,
                    body);
            }

            var selectClauseSyntax = node.Body.SelectOrGroup as SelectClauseSyntax;
            if (selectClauseSyntax == null)
            {
                return base.VisitQueryExpression(node);
            }

            body = body.AddStatements(YieldStatement(SyntaxKind.YieldReturnStatement, selectClauseSyntax.Expression));

            if (innerStmt != null)
            {
                var forEachInnerStmt = ForEachStatement(
                    IdentifierName("var"),
                    innerStmt.Identifier,
                    innerStmt.Expression,
                    body);

                body = Block().AddStatements(forEachInnerStmt);
            }

            var stmt = ForEachStatement(
                IdentifierName("var"),
                node.FromClause.Identifier,
                node.FromClause.Expression,
                body
            );

            if (parent == null)
                return stmt;

            var parentBody = (BlockSyntax)parent.Statement;

            var yieldStatementSyntax = (YieldStatementSyntax)parentBody.Statements.Last();
            var parentVar = LocalDeclarationStatement(
                VariableDeclaration(IdentifierName("var"), SingletonSeparatedList(VariableDeclarator(
                            node.FromClause.Identifier)
                        .WithInitializer(
                            EqualsValueClause(yieldStatementSyntax.Expression)
                        )
                    )
                )
            );
            

            return parent.WithStatement(
                parentBody.ReplaceNode(yieldStatementSyntax, parentVar).AddStatements(stmt.Statement)
            );
        }

        private static ExpressionSyntax StripExpressionParenthesis(ExpressionSyntax expr)
        {
            while (expr is ParenthesizedExpressionSyntax)
            {
                expr = ((ParenthesizedExpressionSyntax)expr).Expression;
            }

            return expr;
        }

        private static SyntaxNode StripExpressionParentParenthesis(SyntaxNode expr)
        {
            if (expr == null)
                return null;
            while (expr.Parent is ParenthesizedExpressionSyntax)
            {
                expr = ((ParenthesizedExpressionSyntax)expr.Parent).Parent;
            }

            return expr.Parent;
        }
    }
}

