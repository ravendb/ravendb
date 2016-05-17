using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Loader;
using System.Text;
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

        private static  MetadataReference[] references =
            {
                MetadataReference.CreateFromFile(typeof (object).GetTypeInfo().Assembly.Location),
                MetadataReference.CreateFromFile(Assembly.Load(new AssemblyName("System.Runtime")).Location),
                MetadataReference.CreateFromFile(typeof (Enumerable).GetTypeInfo().Assembly.Location),
                MetadataReference.CreateFromFile(typeof (StaticIndexCompiler).GetTypeInfo().Assembly.Location),
            };

        private static SyntaxTree baseSyntaxTree = CSharpSyntaxTree.ParseText(@"

using System;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;

namespace Raven.Server.Documents.Indexes.Static.Generated
{
    public class WillBeReplaced : StaticIndexBase
    {
        public WillBeReplaced()
        {
        }
    }
}

");


        public void Compile(IndexDefinition definition)
        {
            var cSharpSafeName = GetCSharpSafeName(definition);
            var syntaxNode = new ChangeClassName(cSharpSafeName).Visit(baseSyntaxTree.GetRoot());

            var syntaxTree = SyntaxFactory.SyntaxTree(syntaxNode);

            CSharpCompilation compilation = CSharpCompilation.Create(
                assemblyName: definition.Name + ".index.dll",
                syntaxTrees: new[] { syntaxTree },
                references: references,
                options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

            var asm = new MemoryStream();
            var pdb = new MemoryStream();

            var result = compilation.Emit(asm, pdb);

            if (result.Success == false)
            {
                IEnumerable<Diagnostic> failures = result.Diagnostics.Where(diagnostic =>
                 diagnostic.IsWarningAsError ||
                 diagnostic.Severity == DiagnosticSeverity.Error);

                var sb = new StringBuilder();
                sb.AppendLine($"Failed to compile index {definition.Name}");
                sb.AppendLine();
                foreach (Diagnostic diagnostic in failures)
                {
                    sb.AppendLine($"{diagnostic.Id}: {diagnostic.GetMessage(CultureInfo.InvariantCulture)}");
                }

                throw new IndexCompilationException(sb.ToString());
            }
            asm.Position = 0;
            pdb.Position = 0;
            var indexAssembly = AssemblyLoadContext.Default.LoadFromStream(asm,pdb);

            var type = indexAssembly.GetType("Raven.Server.Documents.Indexes.Static.Generated." + cSharpSafeName);

            var index = (StaticIndexBase)Activator.CreateInstance(type);


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

    internal class ChangeClassName : CSharpSyntaxRewriter
    {
        private readonly string _name;

        public ChangeClassName(string name)
        {
            _name = name;
        }

        public override SyntaxNode VisitClassDeclaration(ClassDeclarationSyntax node)
        {
            return node.WithIdentifier(SyntaxFactory.Identifier(_name));
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