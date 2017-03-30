using System;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using static Microsoft.CodeAnalysis.CSharp.SyntaxFactory;
using System.Linq;
using System.Linq.Expressions;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public class RavenLinqPrettifier : CSharpSyntaxRewriter
    {
        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            var memeberAccess = node.Expression as MemberAccessExpressionSyntax;
            if (memeberAccess == null || node.ArgumentList.Arguments.Count < 1 || !AllParentsAreMethods(node))
                return base.VisitInvocationExpression(node);

            var expressionSyntax = GetSimpleLambdaExpressionSyntax(node);
            if (expressionSyntax == null || memeberAccess.Name.Identifier.ValueText != "Select")            
                return base.VisitInvocationExpression(node);
            
            var invocExp = memeberAccess.Expression as InvocationExpressionSyntax;
            MemberAccessExpressionSyntax innerMemberAccess = null;
            if (invocExp != null)
                innerMemberAccess = invocExp.Expression as MemberAccessExpressionSyntax;

            if (innerMemberAccess == null || innerMemberAccess.Name.Identifier.ValueText != "Where")
                return QueryExpression(
                    FromClause(
                        expressionSyntax.Parameter.Identifier,
                        RavenLinqOptimizer.MaybeParenthesizedExpression((ExpressionSyntax)Visit(memeberAccess.Expression))
                    ),
                    QueryBody(SelectClause((ExpressionSyntax)Visit(expressionSyntax.Body))));

            var identifierNameSyntax = innerMemberAccess.Expression as IdentifierNameSyntax;
            if (identifierNameSyntax == null)
                return base.VisitInvocationExpression(node);

            var whereClause = GetSimpleLambdaExpressionSyntax(invocExp);
                whereClause = whereClause.WithParameter(expressionSyntax.Parameter)
                    .WithBody(whereClause.Body.ReplaceNodes(whereClause.Body.DescendantNodes().OfType<IdentifierNameSyntax>(), (orig, _) =>
                    {
                        if (orig.Parent is MemberAccessExpressionSyntax access  && orig == access.Name)
                            return orig;

                        if (orig.Identifier.ValueText != whereClause.Parameter.Identifier.ValueText)
                            return orig;
                        return orig.WithIdentifier(expressionSyntax.Parameter.Identifier);
                    }));

            return QueryExpression(
                FromClause(
                    expressionSyntax.Parameter.Identifier,
                    IdentifierName(identifierNameSyntax.Identifier.ValueText)
                ),
                QueryBody(SelectClause((ExpressionSyntax)Visit(expressionSyntax.Body)))
                    .WithClauses(SingletonList<QueryClauseSyntax>(WhereClause((ExpressionSyntax)Visit(whereClause.Body)))));
            
        }

        private static SimpleLambdaExpressionSyntax GetSimpleLambdaExpressionSyntax(InvocationExpressionSyntax node)
        {
            SimpleLambdaExpressionSyntax expressionSyntax;
            var castExpressionSyntax = node.ArgumentList.Arguments[0].Expression as CastExpressionSyntax;

            if (castExpressionSyntax != null)
                expressionSyntax = RavenLinqOptimizer.StripExpressionParenthesis(castExpressionSyntax.Expression) as SimpleLambdaExpressionSyntax;
            else
                expressionSyntax = node.ArgumentList.Arguments[0].Expression as SimpleLambdaExpressionSyntax;
            return expressionSyntax;
        }

        private static bool AllParentsAreMethods(InvocationExpressionSyntax node)
        {
            var es = node.Parent;

            while (es != null)
            {
                if (!(es is InvocationExpressionSyntax))
                    return false;
                es = es.Parent;
            }

            return true;
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

            var dummyYield = YieldStatement(SyntaxKind.YieldReturnStatement, LiteralExpression(SyntaxKind.NullLiteralExpression));

            var body = Block().AddStatements(dummyYield);

            foreach (var clause in node.Body.Clauses)
            {
                var whereClauseSyntax = clause as WhereClauseSyntax;
                if (whereClauseSyntax != null)
                {

                    body = body.InsertNodesBefore(FindDummyYieldIn(body), new[]{
                        IfStatement(BinaryExpression(SyntaxKind.EqualsExpression, MaybeParenthesizedExpression(whereClauseSyntax.Condition), LiteralExpression(SyntaxKind.FalseLiteralExpression)), ContinueStatement())
                    });
                    continue;
                }
                var letClauseSyntax = clause as LetClauseSyntax;
                if (letClauseSyntax != null)
                {
                    body = body.InsertNodesBefore(FindDummyYieldIn(body), new[]{
                     LocalDeclarationStatement(
                            VariableDeclaration(IdentifierName("var"), SingletonSeparatedList(VariableDeclarator(
                                        letClauseSyntax.Identifier)
                                    .WithInitializer(
                                        EqualsValueClause(letClauseSyntax.Expression)
                                    )
                                )
                            )
                        )
                    });
                    continue;
                }
                var fromClauseSyntax = clause as FromClauseSyntax;
                if (fromClauseSyntax != null)
                {
                    var nestedStmt = ForEachStatement(
                        IdentifierName("var"),
                        fromClauseSyntax.Identifier,
                        fromClauseSyntax.Expression,
                        Block().AddStatements(dummyYield));

                    body = body.ReplaceNode(FindDummyYieldIn(body), nestedStmt);

                    continue;
                }
                return base.VisitQueryExpression(node);
            }

            var selectClauseSyntax = node.Body.SelectOrGroup as SelectClauseSyntax;
            if (selectClauseSyntax == null)
            {
                return base.VisitQueryExpression(node);
            }


            var stmt = ForEachStatement(
                IdentifierName("var"),
                node.FromClause.Identifier,
                node.FromClause.Expression,
                body
            );

            stmt = stmt.ReplaceNode(FindDummyYieldIn(stmt),
                YieldStatement(SyntaxKind.YieldReturnStatement, selectClauseSyntax.Expression)
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

        private static YieldStatementSyntax FindDummyYieldIn(SyntaxNode parent)
        {
            var arr = parent.DescendantNodes().Where(n =>
            {
                var token = (((n as YieldStatementSyntax)?.Expression as LiteralExpressionSyntax)?.Token)?.Kind();
                return token == SyntaxKind.NullKeyword;
            }).ToArray();
            return arr.Last() as YieldStatementSyntax;
        }

        internal static ExpressionSyntax MaybeParenthesizedExpression(ExpressionSyntax es)
        {
            if (es is MemberAccessExpressionSyntax)
                return es;

            if (es is IdentifierNameSyntax)
                return es;

            return ParenthesizedExpression(es);
        }

        internal static ExpressionSyntax StripExpressionParenthesis(ExpressionSyntax expr)
        {
            while (expr is ParenthesizedExpressionSyntax)
            {
                expr = ((ParenthesizedExpressionSyntax)expr).Expression;
            }

            return expr;
        }

        internal static SyntaxNode StripExpressionParentParenthesis(SyntaxNode expr)
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

