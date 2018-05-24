using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Formatting;
using Raven.Client.Documents.Indexes;
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
            UsingDirective(IdentifierName(Identifier("System"))),
            UsingDirective(IdentifierName("System.Collections")),
            UsingDirective(IdentifierName("System.Collections.Generic")),
            UsingDirective(IdentifierName("System.Text.RegularExpressions")),
            UsingDirective(IdentifierName("System.Globalization")),
            UsingDirective(IdentifierName("System.Linq")),
            UsingDirective(IdentifierName("Raven.Client.Documents.Indexes"))
        };

        public IndexDefinitionCodeGenerator(IndexDefinition indexDefinition)
        {
            _indexDefinition = indexDefinition;
        }

        public string Generate()
        {
            return GetText(_indexDefinition.Name, _indexDefinition);
        }

        private static string GetText(string indexName, IndexDefinition indexDefinition)
        {
            var usings = RoslynHelper.CreateUsings(Usings);

            // Create a IndexName get property
            PropertyDeclarationSyntax indexNameProperty =
                PropertyDeclaration(PredefinedType(Token(TriviaList(), SyntaxKind.StringKeyword, TriviaList(Space))),
                        Identifier(TriviaList(), IndexName, TriviaList(Space)))
                    .WithModifiers(TokenList(Token(TriviaList(Tab), SyntaxKind.PublicKeyword, TriviaList(Space)),
                        Token(TriviaList(), SyntaxKind.OverrideKeyword, TriviaList(Space))))
                    .WithExpressionBody(ArrowExpressionClause(
                            LiteralExpression(SyntaxKind.StringLiteralExpression,
                                Literal($"{indexName}")))
                        .WithArrowToken(Token(TriviaList(), SyntaxKind.EqualsGreaterThanToken, TriviaList(Space))))
                    .WithSemicolonToken(Token(TriviaList(), SyntaxKind.SemicolonToken, TriviaList(LineFeed, LineFeed)));

            MethodDeclarationSyntax createIndexDefinition =
                MethodDeclaration(IdentifierName(Identifier(TriviaList(), "IndexDefinition", TriviaList(Space))),
                        Identifier(CreateIndexDefinition))
                    .WithModifiers(TokenList(Token(TriviaList(Tab), SyntaxKind.PublicKeyword, TriviaList(Space)),
                        Token(TriviaList(), SyntaxKind.OverrideKeyword, TriviaList(Space))))
                    .WithParameterList(ParameterList().WithCloseParenToken(Token(TriviaList(), SyntaxKind.CloseParenToken, TriviaList(LineFeed))))
                    .WithBody(Block(
                        SingletonList<StatementSyntax>(ReturnStatement(
                                ObjectCreationExpression(IdentifierName(Identifier(TriviaList(), "IndexDefinition", TriviaList(LineFeed))))
                                    .WithNewKeyword(Token(TriviaList(), SyntaxKind.NewKeyword, TriviaList(Space)))
                                    .WithInitializer(InitializerExpression(SyntaxKind.ObjectInitializerExpression,
                                            SeparatedList<ExpressionSyntax>(ParsingIndexDefinitionFields(indexDefinition)))
                                        .WithOpenBraceToken(Token(TriviaList(Tab, Tab), SyntaxKind.OpenBraceToken, TriviaList(LineFeed)))
                                        .WithCloseBraceToken(Token(TriviaList(LineFeed, Tab, Tab), SyntaxKind.CloseBraceToken, TriviaList()))))
                            .WithReturnKeyword(Token(TriviaList(Tab, Tab), SyntaxKind.ReturnKeyword, TriviaList(Space)))
                            .WithSemicolonToken(Token(TriviaList(), SyntaxKind.SemicolonToken, TriviaList(LineFeed)))))
                            .WithOpenBraceToken(Token(TriviaList(Tab), SyntaxKind.OpenBraceToken, TriviaList(LineFeed)))
                            .WithCloseBraceToken(Token(TriviaList(Tab), SyntaxKind.CloseBraceToken, TriviaList(LineFeed))));


            ClassDeclarationSyntax c = ClassDeclaration(Identifier(TriviaList(), GetCSharpSafeName(indexName), TriviaList(Space)))
                .AddModifiers(Token(TriviaList(LineFeed, LineFeed), SyntaxKind.PublicKeyword, TriviaList(Space)))
                .WithKeyword(Token(TriviaList(), SyntaxKind.ClassKeyword, TriviaList(Space)))
                .AddBaseListTypes(SimpleBaseType(IdentifierName(Identifier(TriviaList(), "AbstractIndexCreationTask", TriviaList(LineFeed)))))
                .WithOpenBraceToken(Token(TriviaList(), SyntaxKind.OpenBraceToken, TriviaList(LineFeed)))
                .WithMembers(List(new List<MemberDeclarationSyntax> { indexNameProperty, createIndexDefinition }));

            CompilationUnitSyntax cu = CompilationUnit()
                .WithUsings(usings)
                .NormalizeWhitespace()
                .AddMembers(c);

            SyntaxNode formatedCompilationUnit;
            using (var workspace = new AdhocWorkspace())
            {
                formatedCompilationUnit = Formatter.Format(cu, workspace);
            }

            return formatedCompilationUnit.ToFullString();
        }

        private static IEnumerable<SyntaxNodeOrToken> ParsingIndexDefinitionFields(IndexDefinition indexDefinition)
        {
            var syntaxNodeOrToken = new List<SyntaxNodeOrToken>();

            var maps = AssignmentExpression(SyntaxKind.SimpleAssignmentExpression,
                IdentifierName(Identifier(TriviaList(Tab, Tab, Tab), "Maps", TriviaList(Space))),
                InitializerExpression(SyntaxKind.CollectionInitializerExpression,
                        SeparatedList<ExpressionSyntax>(ParsingIndexDefinitionMapsToRoslyn(indexDefinition.Maps)))
                    .WithOpenBraceToken(Token(TriviaList(Tab, Tab, Tab), SyntaxKind.OpenBraceToken, TriviaList(LineFeed)))
                    .WithCloseBraceToken(Token(TriviaList(LineFeed, Tab, Tab, Tab), SyntaxKind.CloseBraceToken, TriviaList())))
                    .WithOperatorToken(Token(TriviaList(), SyntaxKind.EqualsToken, TriviaList(LineFeed)));

            if (maps != null)
                syntaxNodeOrToken.Add(maps);

            if (indexDefinition.Reduce != null)
            {
                syntaxNodeOrToken.Add(Token(TriviaList(), SyntaxKind.CommaToken, TriviaList(LineFeed)));
                syntaxNodeOrToken.Add(GetLiteral("Reduce", indexDefinition.Reduce));
            }

            if (indexDefinition.Fields.Count > 0)
            {
                syntaxNodeOrToken.Add(Token(TriviaList(), SyntaxKind.CommaToken, TriviaList(LineFeed)));
                var fields = AssignmentExpression(SyntaxKind.SimpleAssignmentExpression,
                        IdentifierName(Identifier(TriviaList(Tab, Tab, Tab), "Fields", TriviaList(Space))),
                        InitializerExpression(SyntaxKind.CollectionInitializerExpression,
                        SeparatedList<ExpressionSyntax>(ParsingIndexDefinitionFieldsToRoslyn(indexDefinition.Fields)))
                            .WithOpenBraceToken(Token(TriviaList(Tab, Tab, Tab), SyntaxKind.OpenBraceToken, TriviaList(LineFeed)))
                            .WithCloseBraceToken(Token(TriviaList(LineFeed, Tab, Tab, Tab), SyntaxKind.CloseBraceToken, TriviaList())))
                    .WithOperatorToken(Token(TriviaList(), SyntaxKind.EqualsToken, TriviaList(LineFeed)));

                syntaxNodeOrToken.Add(fields);

            }

            return syntaxNodeOrToken;
        }

        private static AssignmentExpressionSyntax GetLiteral(string fieldName, string field)
        {
            return AssignmentExpression(SyntaxKind.SimpleAssignmentExpression,
                    IdentifierName(Identifier(TriviaList(Tab, Tab, Tab), fieldName, TriviaList(Space))),
                    LiteralExpression(SyntaxKind.StringLiteralExpression,
                        Literal(TriviaList(), $"@\"{field}\"", field, TriviaList())))
                .WithOperatorToken(Token(TriviaList(), SyntaxKind.EqualsToken, TriviaList(Space)));
        }


        private static IEnumerable<SyntaxNodeOrToken> ParsingIndexDefinitionFieldsToRoslyn(Dictionary<string, IndexFieldOptions> fields)
        {
            var syntaxNodeOrToken = new List<SyntaxNodeOrToken>();

            var countFields = 0;
            foreach (var field in fields)
            {
                countFields++;
                var initializerExpression = InitializerExpression(SyntaxKind.ComplexElementInitializerExpression,
                    SeparatedList<ExpressionSyntax>(new SyntaxNodeOrToken[]
                    {
                        LiteralExpression(SyntaxKind.StringLiteralExpression, Literal(field.Key)),
                            Token(TriviaList(), SyntaxKind.CommaToken, TriviaList(Space)),
                            ObjectCreationExpression(IdentifierName(Identifier(TriviaList(), field.Value.GetType().Name, TriviaList(LineFeed))))
                                .WithNewKeyword(Token(TriviaList(), SyntaxKind.NewKeyword, TriviaList(Space)))
                        .WithInitializer(InitializerExpression(SyntaxKind.ObjectInitializerExpression,
                        SeparatedList<ExpressionSyntax>(InnerParsingIndexDefinitionFieldsToRoslyn(field.Value)))
                        .WithOpenBraceToken(Token(TriviaList(Tab, Tab, Tab, Tab), SyntaxKind.OpenBraceToken, TriviaList(LineFeed)))
                        .WithCloseBraceToken(Token(TriviaList(LineFeed, Tab, Tab, Tab, Tab),SyntaxKind.CloseBraceToken, TriviaList(Space))))
                    }))
                    .WithOpenBraceToken(Token(TriviaList(Tab, Tab, Tab, Tab), SyntaxKind.OpenBraceToken, TriviaList()))
                    .WithCloseBraceToken(Token(TriviaList(), SyntaxKind.CloseBraceToken, TriviaList(Space)));

                syntaxNodeOrToken.Add(initializerExpression);
                if (countFields < fields.Count)
                    syntaxNodeOrToken.Add(Token(TriviaList(), SyntaxKind.CommaToken, TriviaList(LineFeed)));
            }

            return syntaxNodeOrToken;
        }

        private static IEnumerable<SyntaxNodeOrToken> InnerParsingIndexDefinitionFieldsToRoslyn(IndexFieldOptions options)
        {
            var syntaxNodeOrToken = new List<SyntaxNodeOrToken>();
            if (options.Indexing != null)
            {
                AddCommaTokenIfNecessary(syntaxNodeOrToken);
                syntaxNodeOrToken.Add(ParseEnum(options.Indexing, nameof(options.Indexing)));
            }
            if (options.Storage != null)
            {
                AddCommaTokenIfNecessary(syntaxNodeOrToken);
                syntaxNodeOrToken.Add(ParseEnum(options.Storage, nameof(options.Storage)));
            }
            if (options.TermVector != null)
            {
                AddCommaTokenIfNecessary(syntaxNodeOrToken);
                syntaxNodeOrToken.Add(ParseEnum(options.TermVector, nameof(options.TermVector)));
            }
            if (!string.IsNullOrEmpty(options.Analyzer))
            {
                AddCommaTokenIfNecessary(syntaxNodeOrToken);
                syntaxNodeOrToken.Add(GetLiteral(nameof(options.Analyzer), options.Analyzer));
            }
            if (options.Suggestions != null)
            {
                AddCommaTokenIfNecessary(syntaxNodeOrToken);
                syntaxNodeOrToken.Add(ParseBool((bool)options.Suggestions, nameof(options.Suggestions)));
            }

            return syntaxNodeOrToken;
        }

        private static void AddCommaTokenIfNecessary(ICollection<SyntaxNodeOrToken> syntaxNodeOrToken)
        {
            if (syntaxNodeOrToken.Count > 0)
                syntaxNodeOrToken.Add(Token(TriviaList(), SyntaxKind.CommaToken, TriviaList(LineFeed)));
        }

        private static AssignmentExpressionSyntax ParseEnum<T>(T option, string fieldName)
        {
            return AssignmentExpression(SyntaxKind.SimpleAssignmentExpression,
                IdentifierName(Identifier(TriviaList(Tab, Tab, Tab, Tab, Tab), fieldName, TriviaList(Space))),
                MemberAccessExpression(SyntaxKind.SimpleMemberAccessExpression, IdentifierName(option.GetType().Name),
                IdentifierName(option.ToString())))
                    .WithOperatorToken(Token(TriviaList(), SyntaxKind.EqualsToken, TriviaList(Space)));
        }

        private static AssignmentExpressionSyntax ParseBool(bool option, string fieldName)
        {
            return AssignmentExpression(SyntaxKind.SimpleAssignmentExpression,
                    IdentifierName(Identifier(TriviaList(Tab, Tab, Tab, Tab, Tab), fieldName, TriviaList(Space))),
                    LiteralExpression(option ? SyntaxKind.TrueLiteralExpression : SyntaxKind.FalseLiteralExpression)
                        .WithToken(Token(TriviaList(), option ? SyntaxKind.TrueKeyword : SyntaxKind.FalseKeyword, TriviaList(LineFeed))))
                .WithOperatorToken(Token(TriviaList(), SyntaxKind.EqualsToken, TriviaList(Space)));
        }

        private static IEnumerable<SyntaxNodeOrToken> ParsingIndexDefinitionMapsToRoslyn(ICollection<string> maps)
        {
            if (maps == null)
                throw new InvalidOperationException(nameof(maps));

            var syntaxNodeOrToken = new List<SyntaxNodeOrToken>();

            var countMap = 0;
            foreach (var map in maps)
            {
                countMap++;
                syntaxNodeOrToken.Add(LiteralExpression(SyntaxKind.StringLiteralExpression,
                    Literal(TriviaList(Tab, Tab, Tab), $"@\"{map.Replace("\"", "\"\"")}\"", map, TriviaList())));
                if (countMap < maps.Count)
                    syntaxNodeOrToken.Add(Token(TriviaList(), SyntaxKind.CommaToken, TriviaList(LineFeed)));
            }
            return syntaxNodeOrToken;
        }

        private static string GetCSharpSafeName(string name)
        {
            return $"Index_{Regex.Replace(name, @"[^\w\d]", "_")}";
        }
    }
}
