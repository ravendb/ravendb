using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

using Raven.Abstractions.Exceptions;
using Raven.Client.Indexing;

namespace Raven.Server.Documents.Indexes.Static
{
    public class StaticIndexCompiler
    {
        private readonly SyntaxTrivia _tab = SyntaxFactory.Whitespace(@"    ");

        public void Compile(IndexDefinition definition)
        {
            var name = GetCSharpSafeName(definition);
            var className = SyntaxFactory.Identifier(SyntaxTriviaList.Empty, name, SyntaxTriviaList.Empty);
            var classModifiers = SyntaxFactory.TokenList(SyntaxFactory.Token(SyntaxTriviaList.Empty, SyntaxKind.PublicKeyword, SyntaxTriviaList.Create(SyntaxFactory.Space)));

            var classKeyword = SyntaxFactory.Token(
                SyntaxTriviaList.Empty,
                SyntaxKind.ClassKeyword,
                SyntaxTriviaList.Create(SyntaxFactory.Space));

            var baseClassName = SyntaxFactory.IdentifierName(SyntaxFactory.Identifier(SyntaxTriviaList.Empty, nameof(StaticIndexBase), SyntaxTriviaList.Create(SyntaxFactory.LineFeed)));
            var baseList = SyntaxFactory.BaseList(SyntaxFactory.SingletonSeparatedList<BaseTypeSyntax>(SyntaxFactory.SimpleBaseType(baseClassName)))
                .WithColonToken(SyntaxFactory.Token(SyntaxTriviaList.Create(SyntaxFactory.Space), SyntaxKind.ColonToken, SyntaxTriviaList.Create(SyntaxFactory.Space)));

            var @class = SyntaxFactory
                .ClassDeclaration(className)
                .WithModifiers(classModifiers)
                .WithKeyword(classKeyword)
                .WithBaseList(baseList)
                .WithOpenBraceToken(SyntaxFactory.Token(SyntaxTriviaList.Empty, SyntaxKind.OpenBraceToken, SyntaxTriviaList.Create(SyntaxFactory.LineFeed)))
                .WithMembers(GenerateMembers(name, definition))
                .WithCloseBraceToken(SyntaxFactory.Token(SyntaxTriviaList.Empty, SyntaxKind.CloseBraceToken, SyntaxTriviaList.Create(SyntaxFactory.LineFeed)));

            var z = @class.ToFullString();
        }

        private SyntaxList<MemberDeclarationSyntax> GenerateMembers(string className, IndexDefinition definition)
        {
            var body = SyntaxFactory.Block()
                .WithOpenBraceToken(SyntaxFactory.Token(SyntaxTriviaList.Empty, SyntaxKind.OpenBraceToken, SyntaxTriviaList.Create(SyntaxFactory.LineFeed)))
                .WithStatements(GenerateStatements(definition))
                .WithCloseBraceToken(SyntaxFactory.Token(SyntaxTriviaList.Create(_tab), SyntaxKind.CloseBraceToken, SyntaxTriviaList.Create(SyntaxFactory.LineFeed)))
                .WithLeadingTrivia(SyntaxFactory.LineFeed, _tab);

            var ctor = SyntaxFactory.ConstructorDeclaration(SyntaxFactory.Identifier(SyntaxTriviaList.Empty, className, SyntaxTriviaList.Empty))
                .WithModifiers(SyntaxFactory.TokenList(SyntaxFactory.Token(SyntaxTriviaList.Create(_tab), SyntaxKind.PublicKeyword, SyntaxTriviaList.Create(SyntaxFactory.Space))))
                .WithBody(body);

            var members = new List<MemberDeclarationSyntax> { ctor };

            return SyntaxFactory.List(members);
        }

        private SyntaxList<StatementSyntax> GenerateStatements(IndexDefinition definition)
        {
            var mapFunctions = HandleMaps(definition);

            return SyntaxFactory.List(mapFunctions);
        }

        private IEnumerable<StatementSyntax> HandleMaps(IndexDefinition definition)
        {
            return definition.Maps.SelectMany(map => HandleMap(map, definition));
        }

        private List<StatementSyntax> HandleMap(string map, IndexDefinition definition)
        {
            try
            {
                var statements = new List<StatementSyntax>();

                var expression = SyntaxFactory.ParseExpression(map);
                var queryExpression = expression as QueryExpressionSyntax;
                if (queryExpression == null)
                    throw new InvalidOperationException();

                var fromClause = queryExpression.FromClause;

                var collectionName = fromClause.Accept(new ExtractCollectionNameVisitor());
                statements.Add(SyntaxFactory.ExpressionStatement(SyntaxFactory.ParseExpression($"this.{nameof(StaticIndexBase.ForCollections)}.Add(\"{collectionName}\")"))
                    .WithLeadingTrivia(_tab, _tab)
                    .WithTrailingTrivia(SyntaxFactory.LineFeed));

                return statements;
            }
            catch (InvalidOperationException ex)
            {
                throw new IndexCompilationException(ex.Message, ex)
                {
                    IndexDefinitionProperty = "Maps",
                    ProblematicText = map
                };
            }
        }

        private string GetCSharpSafeName(IndexDefinition definition)
        {
            return $"Index_{Regex.Replace(definition.Name, @"[^\w\d]", "_")}";
        }
    }

    internal class ExtractCollectionNameVisitor : CSharpSyntaxVisitor<string>
    {
        public override string VisitFromClause(FromClauseSyntax node)
        {
            var docsExpression = node.Expression as MemberAccessExpressionSyntax;
            if (docsExpression == null)
                throw new InvalidOperationException();

            var docsIdentifier = docsExpression.Expression as IdentifierNameSyntax;
            if (string.Equals(docsIdentifier?.Identifier.Text, "docs", StringComparison.OrdinalIgnoreCase) == false)
                throw new InvalidOperationException();

            return docsExpression.Name.Identifier.Text;
        }
    }
}