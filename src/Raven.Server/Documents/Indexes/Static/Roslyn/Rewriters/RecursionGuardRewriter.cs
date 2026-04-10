using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public sealed class RecursionGuardRewriter : CSharpSyntaxRewriter
    {
        private static readonly StatementSyntax EnsureSufficientExecutionStackStatement =
            SyntaxFactory.ParseStatement("System.Runtime.CompilerServices.RuntimeHelpers.EnsureSufficientExecutionStack();\n");

        public override SyntaxNode VisitMethodDeclaration(MethodDeclarationSyntax node)
        {
            node = (MethodDeclarationSyntax)base.VisitMethodDeclaration(node);
            return GuardMethodBody(node, node.Body, node.ExpressionBody,
                IsVoidReturnType(node.ReturnType),
                (n, b) => n.WithBody(b).WithExpressionBody(null).WithSemicolonToken(default),
                (n, b) => n.WithBody(b));
        }

        public override SyntaxNode VisitLocalFunctionStatement(LocalFunctionStatementSyntax node)
        {
            node = (LocalFunctionStatementSyntax)base.VisitLocalFunctionStatement(node);
            return GuardMethodBody(node, node.Body, node.ExpressionBody,
                IsVoidReturnType(node.ReturnType),
                (n, b) => n.WithBody(b).WithExpressionBody(null).WithSemicolonToken(default),
                (n, b) => n.WithBody(b));
        }

        public override SyntaxNode VisitConstructorDeclaration(ConstructorDeclarationSyntax node)
        {
            node = (ConstructorDeclarationSyntax)base.VisitConstructorDeclaration(node);
            return GuardMethodBody(node, node.Body, node.ExpressionBody,
                isVoidReturning: true,
                (n, b) => n.WithBody(b).WithExpressionBody(null).WithSemicolonToken(default),
                (n, b) => n.WithBody(b));
        }

        public override SyntaxNode VisitDestructorDeclaration(DestructorDeclarationSyntax node)
        {
            node = (DestructorDeclarationSyntax)base.VisitDestructorDeclaration(node);
            return GuardMethodBody(node, node.Body, node.ExpressionBody,
                isVoidReturning: true,
                (n, b) => n.WithBody(b).WithExpressionBody(null).WithSemicolonToken(default),
                (n, b) => n.WithBody(b));
        }

        public override SyntaxNode VisitOperatorDeclaration(OperatorDeclarationSyntax node)
        {
            node = (OperatorDeclarationSyntax)base.VisitOperatorDeclaration(node);
            return GuardMethodBody(node, node.Body, node.ExpressionBody,
                isVoidReturning: false,
                (n, b) => n.WithBody(b).WithExpressionBody(null).WithSemicolonToken(default),
                (n, b) => n.WithBody(b));
        }

        public override SyntaxNode VisitConversionOperatorDeclaration(ConversionOperatorDeclarationSyntax node)
        {
            node = (ConversionOperatorDeclarationSyntax)base.VisitConversionOperatorDeclaration(node);
            return GuardMethodBody(node, node.Body, node.ExpressionBody,
                isVoidReturning: false,
                (n, b) => n.WithBody(b).WithExpressionBody(null).WithSemicolonToken(default),
                (n, b) => n.WithBody(b));
        }

        public override SyntaxNode VisitAccessorDeclaration(AccessorDeclarationSyntax node)
        {
            node = (AccessorDeclarationSyntax)base.VisitAccessorDeclaration(node);
            bool isVoid = node.Kind() != SyntaxKind.GetAccessorDeclaration;
            return GuardMethodBody(node, node.Body, node.ExpressionBody,
                isVoid,
                (n, b) => n.WithBody(b).WithExpressionBody(null).WithSemicolonToken(default),
                (n, b) => n.WithBody(b));
        }

        public override SyntaxNode VisitPropertyDeclaration(PropertyDeclarationSyntax node)
        {
            node = (PropertyDeclarationSyntax)base.VisitPropertyDeclaration(node);

            if (node.ExpressionBody == null)
                return node;

            var getter = SyntaxFactory.AccessorDeclaration(SyntaxKind.GetAccessorDeclaration)
                .WithBody(SyntaxFactory.Block(
                    EnsureSufficientExecutionStackStatement,
                    ConvertExpressionToStatement(node.ExpressionBody.Expression, isVoidReturning: false)));

            return node
                .WithAccessorList(SyntaxFactory.AccessorList(SyntaxFactory.SingletonList(getter)))
                .WithExpressionBody(null)
                .WithSemicolonToken(default);
        }

        public override SyntaxNode VisitIndexerDeclaration(IndexerDeclarationSyntax node)
        {
            node = (IndexerDeclarationSyntax)base.VisitIndexerDeclaration(node);

            if (node.ExpressionBody == null)
                return node;

            var getter = SyntaxFactory.AccessorDeclaration(SyntaxKind.GetAccessorDeclaration)
                .WithBody(SyntaxFactory.Block(
                    EnsureSufficientExecutionStackStatement,
                    ConvertExpressionToStatement(node.ExpressionBody.Expression, isVoidReturning: false)));

            return node
                .WithAccessorList(SyntaxFactory.AccessorList(SyntaxFactory.SingletonList(getter)))
                .WithExpressionBody(null)
                .WithSemicolonToken(default);
        }

        private static T GuardMethodBody<T>(T node, BlockSyntax body, ArrowExpressionClauseSyntax expressionBody,
            bool isVoidReturning,
            System.Func<T, BlockSyntax, T> withExpressionBodyReplacement,
            System.Func<T, BlockSyntax, T> withBodyReplacement) where T : SyntaxNode
        {
            if (body != null)
            {
                return withBodyReplacement(node,
                    body.WithStatements(
                        body.Statements.Insert(0, EnsureSufficientExecutionStackStatement)));
            }

            if (expressionBody != null)
            {
                var block = SyntaxFactory.Block(EnsureSufficientExecutionStackStatement,
                    ConvertExpressionToStatement(expressionBody.Expression, isVoidReturning));
                return withExpressionBodyReplacement(node, block);
            }

            return node;
        }

        private static StatementSyntax ConvertExpressionToStatement(ExpressionSyntax expression, bool isVoidReturning)
        {
            if (expression is ThrowExpressionSyntax throwExpression)
                return SyntaxFactory.ThrowStatement(throwExpression.Expression);

            if (isVoidReturning)
                return SyntaxFactory.ExpressionStatement(expression);

            return SyntaxFactory.ReturnStatement(expression);
        }

        private static bool IsVoidReturnType(TypeSyntax returnType)
        {
            return returnType is PredefinedTypeSyntax predefined && predefined.Keyword.IsKind(SyntaxKind.VoidKeyword);
        }
    }
}
