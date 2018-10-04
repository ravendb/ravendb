using System;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using static Microsoft.CodeAnalysis.CSharp.SyntaxFactory;
using System.Linq;
using Raven.Client.Util;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    internal class RavenLinqOptimizer : CSharpSyntaxRewriter
    {
        private readonly FieldNamesValidator _validator;

        public RavenLinqOptimizer(FieldNamesValidator validator)
        {
            if (validator.Fields == null || validator.Fields.Length == 0)
                throw new InvalidOperationException("Validator should have been validating original indexing func");

            _validator = validator;
        }

        private int _recursiveCallCounter;

        public IDisposable RecursiveCall()
        {
            _recursiveCallCounter++;

            return new DisposableAction(() =>
            {
                _recursiveCallCounter--;
            });
        }

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
                using (RecursiveCall())
                {
                    parent = VisitQueryExpression(queryExpressionSyntax) as ForEachStatementSyntax;
                }

                if (parent != null)
                    node =
                        node.WithFromClause(
                            node.FromClause.WithExpression(IdentifierName(queryExpressionSyntax.FromClause.Identifier)));
            }

            var dummyYield = YieldStatement(SyntaxKind.YieldReturnStatement, LiteralExpression(SyntaxKind.NullLiteralExpression));

            var body = Block().AddStatements(dummyYield);

            foreach (var clause in node.Body.Clauses)
            {
                if (TryRewriteBodyClause(clause, dummyYield, ref body))
                    continue;

                return base.VisitQueryExpression(node);
            }

            var selectClauseSyntax = node.Body.SelectOrGroup as SelectClauseSyntax;
            if (selectClauseSyntax == null)
            {
                return base.VisitQueryExpression(node);
            }

            var continuation = node.Body.Continuation;
            while (continuation != null)
            {
                // select new {  } into 

                var selectIntoVar = LocalDeclarationStatement(
                    VariableDeclaration(IdentifierName("var"), SingletonSeparatedList(VariableDeclarator(
                                continuation.Identifier)
                            .WithInitializer(
                                EqualsValueClause(selectClauseSyntax.Expression)
                            )
                        )
                    )
                );

                selectClauseSyntax = continuation.Body.SelectOrGroup as SelectClauseSyntax;

                if (selectClauseSyntax == null)
                {
                    return base.VisitQueryExpression(node);
                }

                body = body.InsertNodesBefore(FindDummyYieldIn(body), new[] { selectIntoVar });

                foreach (var clause in continuation.Body.Clauses)
                {
                    if (TryRewriteBodyClause(clause, dummyYield, ref body))
                        continue;

                    return base.VisitQueryExpression(node);
                }

                continuation = continuation.Body.Continuation;
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
            {
                if (_recursiveCallCounter == 0 && _validator.Validate(stmt.ToFullString(), selectClauseSyntax.Expression, throwOnError: false) == false)
                    ThrowIndexRewritingException(node, stmt);

                return stmt;
            }

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

            var statementSyntax = stmt.Statement;
            if (statementSyntax is BlockSyntax bs)
            {
                statementSyntax = bs.Statements.Single();
            }

            return parent.WithStatement(
                parentBody.ReplaceNode(yieldStatementSyntax, parentVar).AddStatements(statementSyntax)
            );
        }

        private void ThrowIndexRewritingException(QueryExpressionSyntax node, ForEachStatementSyntax stmt)
        {
            throw new InvalidOperationException("Rewriting the function to an optimized version resulted in creating invalid indexing outputs. " +
                                                $"The output needs to have the following fields: [{string.Join(", ", _validator.Fields.Select(x => x.Name))}] " +
                                                $"while after the optimization it has: [{string.Join(", ", _validator.ExtractedFields.Select(x => x.Name))}].{Environment.NewLine}" +
                                                $"Original indexing func:{Environment.NewLine}{node.ToFullString()}{Environment.NewLine}{Environment.NewLine}" +
                                                $"Optimized indexing func:{Environment.NewLine}{stmt.ToFullString()}");
        }

        private static bool TryRewriteBodyClause(QueryClauseSyntax clause, YieldStatementSyntax dummyYield, ref BlockSyntax body)
        {
            if (clause is WhereClauseSyntax whereClauseSyntax)
            {
                body = body.InsertNodesBefore(FindDummyYieldIn(body), new[]
                {
                    IfStatement(
                        BinaryExpression(SyntaxKind.EqualsExpression, MaybeParenthesizedExpression(whereClauseSyntax.Condition),
                            LiteralExpression(SyntaxKind.FalseLiteralExpression)), ContinueStatement())
                });
                return true;
            }

            if (clause is LetClauseSyntax letClauseSyntax)
            {
                body = body.InsertNodesBefore(FindDummyYieldIn(body), new[]
                {
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
                return true;
            }
            if (clause is FromClauseSyntax fromClauseSyntax)
            {
                var nestedStmt = ForEachStatement(
                    IdentifierName("var"),
                    fromClauseSyntax.Identifier,
                    fromClauseSyntax.Expression,
                    Block().AddStatements(dummyYield));

                body = body.ReplaceNode(FindDummyYieldIn(body), nestedStmt);

                return true;
            }
            return false;
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

