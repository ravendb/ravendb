using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using System.Linq;

namespace Raven.Server.Documents.Indexes.Static
{
    public static class RoslynHelper
    {
        public static SyntaxList<UsingDirectiveSyntax> CreateUsings(UsingDirectiveSyntax[] usings)
        {
            return SyntaxFactory.List(usings);
        }

        public static ClassDeclarationSyntax PublicClass(string className)
        {
            return SyntaxFactory.ClassDeclaration(className)
                .WithModifiers(SyntaxFactory.TokenList(SyntaxFactory.Token(SyntaxKind.PublicKeyword)));
        }

        public static ConstructorDeclarationSyntax PublicCtor(string className)
        {
            return SyntaxFactory.ConstructorDeclaration(className)
                .WithModifiers(SyntaxFactory.TokenList(SyntaxFactory.Token(SyntaxKind.PublicKeyword)));
        }

        public static NamespaceDeclarationSyntax CreateNamespace(string @namespace)
        {
            return SyntaxFactory.NamespaceDeclaration(SyntaxFactory.IdentifierName(@namespace));
        }

        public static MemberAccessExpressionSyntax This(string methodName)
        {
            return SyntaxFactory.MemberAccessExpression(SyntaxKind.SimpleMemberAccessExpression, SyntaxFactory.ThisExpression(), SyntaxFactory.IdentifierName(methodName));
        }

        public static AssignmentExpressionSyntax Assign(this MemberAccessExpressionSyntax member, ExpressionSyntax value)
        {
            return SyntaxFactory.AssignmentExpression(SyntaxKind.SimpleAssignmentExpression, member, value);
        }

        public static AssignmentExpressionSyntax Assign(this MemberAccessExpressionSyntax member, string value)
        {
            return member.Assign(SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, SyntaxFactory.Literal(value)));
        }

        public static ClassDeclarationSyntax WithBaseClass<T>(this ClassDeclarationSyntax @class)
        {
            return @class.WithBaseList(SyntaxFactory.BaseList(SyntaxFactory.SingletonSeparatedList<BaseTypeSyntax>(SyntaxFactory.SimpleBaseType(SyntaxFactory.IdentifierName(typeof(T).Name)))));
        }

        public static ExpressionStatementSyntax AsExpressionStatement(this ExpressionSyntax syntax)
        {
            return SyntaxFactory.ExpressionStatement(syntax);
        }

        public static InvocationExpressionSyntax Invoke(this MemberAccessExpressionSyntax member, params ExpressionSyntax[] arguments)
        {
            return SyntaxFactory.InvocationExpression(member, SyntaxFactory.ArgumentList(SyntaxFactory.SeparatedList(arguments.Select(SyntaxFactory.Argument))));
        }
    }
}