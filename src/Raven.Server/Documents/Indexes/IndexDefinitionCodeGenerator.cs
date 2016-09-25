using Raven.Client.Indexing;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Raven.Abstractions.Extensions;
using Raven.Client.Indexes;
using Raven.Server.Documents.Indexes.Static.Roslyn;
using static Microsoft.CodeAnalysis.CSharp.SyntaxFactory;

namespace Raven.Server.Documents.Indexes
{

    public class IndexDefinitionCodeGenerator
    {
        private readonly IndexDefinition _indexDefinition;
        private const string IndexName = "IndexName";
        private const string CreateIndexDefinition = "CreateIndexDefinition";

        private static readonly UsingDirectiveSyntax[] Usings =
        {
            UsingDirective(IdentifierName("System")),
            UsingDirective(IdentifierName("System.Collections")),
            UsingDirective(IdentifierName("System.Collections.Generic")),
            UsingDirective(IdentifierName("System.Text.RegularExpressions")),
            UsingDirective(IdentifierName("System.Globalization")),
            UsingDirective(IdentifierName("System.Linq")),

            UsingDirective(IdentifierName("Raven.Abstractions")),
            UsingDirective(IdentifierName("Raven.Abstractions.Indexing")),
            UsingDirective(IdentifierName("Raven.Abstractions.Data")),
            UsingDirective(IdentifierName("Raven.Client.Indexes"))
        };

        public IndexDefinitionCodeGenerator(IndexDefinition indexDefinition)
        {
            _indexDefinition = indexDefinition;
        }

        public string Generate()
        {
            var indexClass = CreateClass(_indexDefinition.Name, _indexDefinition);
            return GetText(indexClass);
        }

        private static MemberDeclarationSyntax CreateClass(string indexName, IndexDefinition indexDefinition)
        {
            var safeName = GetCSharpSafeName(indexName);

            //Create a class
            var @class = RoslynHelper.PublicClass(safeName)
                .WithBaseClass<AbstractIndexCreationTask>();

            // Create a IndexName get property
            PropertyDeclarationSyntax indexNameProperty =
                PropertyDeclaration(ParseTypeName("string"), Identifier(IndexName))
                .AddModifiers(Token(SyntaxKind.PublicKeyword), Token(SyntaxKind.OverrideKeyword))
                .AddAccessorListAccessors(
                    AccessorDeclaration(SyntaxKind.GetAccessorDeclaration, Block(
                        List(new[] { ReturnStatement(IdentifierName($"\"{indexName}\"")) }
                        ))));

            // Add the property to the class
            @class = @class.AddMembers(indexNameProperty);

            //Create CreateIndexDefinition method
            MethodDeclarationSyntax indexDefinitionMethod =
                MethodDeclaration(ParseTypeName(typeof(IndexDefinition).Name), Identifier(CreateIndexDefinition))
                    .AddModifiers(Token(SyntaxKind.PublicKeyword), Token(SyntaxKind.OverrideKeyword))
                    .WithBody(Block(SingletonList<StatementSyntax>(
                        ReturnStatement(ObjectCreationExpression(IdentifierName(typeof(IndexDefinition).Name))
                        .WithInitializer(InitializerExpression(SyntaxKind.ObjectInitializerExpression,
                        SeparatedList<ExpressionSyntax>(CreateStatements(indexDefinition))))))));

            // Add the Method to the class
            @class = @class.AddMembers(indexDefinitionMethod);

            return @class;
        }

        private static string GetText(MemberDeclarationSyntax indexClassDefinition)
        {
            StringBuilder sb = new StringBuilder();
            var u = RoslynHelper.CreateUsings(Usings);

            u.ForEach(item => sb.Append($"{item.NormalizeWhitespace()}{Environment.NewLine}"));
            sb.Append(Environment.NewLine);
            sb.Append(indexClassDefinition.NormalizeWhitespace().ToFullString());

            return sb.ToString();
        }

        private static List<AssignmentExpressionSyntax> CreateStatements(IndexDefinition indexDefinition)
        {
            List<AssignmentExpressionSyntax> expressions = new List<AssignmentExpressionSyntax>();

            var maps = indexDefinition.Maps.ToList();
            if (maps.Count > 0)
            {
                expressions.Add(
                    AssignmentExpression(SyntaxKind.SimpleAssignmentExpression,
                        IdentifierName("Maps"),
                        InitializerExpression(
                            SyntaxKind.CollectionInitializerExpression,
                            SeparatedList<ExpressionSyntax>(maps.Select(
                            map =>
                                LiteralExpression(SyntaxKind.StringLiteralExpression,
                                    Literal(CleanString(map))))))
                    .WithOpenBraceToken(Token(SyntaxKind.OpenBraceToken))
                    .WithCloseBraceToken(Token(SyntaxKind.CloseBraceToken))));
            }

            if (indexDefinition.Reduce != null)
            {
                var reduce = CleanString(indexDefinition.Reduce);
                expressions.Add(AssignmentExpression(SyntaxKind.SimpleAssignmentExpression,
                IdentifierName("Reduce"), LiteralExpression(
                        SyntaxKind.StringLiteralExpression, Literal(reduce))));
            }

            return expressions;
        }

        private static string GetCSharpSafeName(string name)
        {
            return $"Index_{Regex.Replace(name, @"[^\w\d]", "_")}";
        }

        private static string CleanString(string item)
        {
            return Regex.Replace(item, @"(\\r\\n)?(\s{2,})?", "");
        }
    }
}
