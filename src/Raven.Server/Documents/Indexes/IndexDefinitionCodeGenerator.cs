using System;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Raven.Client.Documents.Indexes;
using Raven.Client.Extensions;
using Raven.Server.Documents.Indexes.Static.Roslyn;
using static Microsoft.CodeAnalysis.CSharp.SyntaxFactory;

namespace Raven.Server.Documents.Indexes
{

    public class IndexDefinitionCodeGenerator
    {
        private readonly IndexDefinition _indexDefinition;
        private const string IndexName = "IndexName";

        private static readonly UsingDirectiveSyntax[] Usings =
        {
            UsingDirective(IdentifierName("System")),
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
            StringBuilder sb = new StringBuilder();
            var usings = RoslynHelper.CreateUsings(Usings);

                // Create a IndexName get property
            PropertyDeclarationSyntax indexNameProperty =
                PropertyDeclaration(ParseTypeName("string"), Identifier(IndexName))
                .AddModifiers(Token(SyntaxKind.PublicKeyword), Token(SyntaxKind.OverrideKeyword))
                .AddAccessorListAccessors(
                    AccessorDeclaration(SyntaxKind.GetAccessorDeclaration, Block(
                        List(new[] { ReturnStatement(IdentifierName($"\"{indexName}\"")) }
                        ))));

            var maps = indexDefinition.Maps.ToList();
            var safeName = GetCSharpSafeName(indexName);
            var nl = Environment.NewLine;

            usings.ForEach(item => sb.Append($"{item.NormalizeWhitespace()}{nl}"));
            sb.Append($"{nl}public class {safeName} : AbstractIndexCreationTask{nl}{{{nl}");
            sb.Append($"{indexNameProperty.NormalizeWhitespace()}{nl}{nl}");
            sb.Append($"public override IndexDefinition CreateIndexDefinition(){nl}{{{nl}");
            sb.Append($"return new IndexDefinition{nl}{{{nl}");
            sb.Append($"Maps = {{@");
            var mapsWrite = 0;
            foreach (var map in maps)
            {
                sb.Append($"\"{map}\"");
                mapsWrite++;
                if (mapsWrite != maps.Count)
                    sb.Append($",{nl}");
            }
            sb.Append($"}},{nl}");

            if (indexDefinition.Reduce != null)
            {
                sb.Append($"Reduce = @\"{indexDefinition.Reduce}\"");
            }

            sb.Append($"{nl}}};{nl}}}{nl}}}");
            return sb.ToString();
        }

        private static string GetCSharpSafeName(string name)
        {
            return $"Index_{Regex.Replace(name, @"[^\w\d]", "_")}";
        }
    }
}
