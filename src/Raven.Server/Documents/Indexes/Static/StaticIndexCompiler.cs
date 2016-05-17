using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Loader;
using System.Text;
using System.Text.RegularExpressions;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;
using Microsoft.CSharp.RuntimeBinder;
using Raven.Abstractions.Exceptions;
using Raven.Client.Indexing;

namespace Raven.Server.Documents.Indexes.Static
{
    public class StaticIndexCompiler
    {
        private static readonly MetadataReference[] References =
            {
                MetadataReference.CreateFromFile(typeof (object).GetTypeInfo().Assembly.Location),
                MetadataReference.CreateFromFile(typeof (Enumerable).GetTypeInfo().Assembly.Location),
                MetadataReference.CreateFromFile(typeof (StaticIndexCompiler).GetTypeInfo().Assembly.Location),
                MetadataReference.CreateFromFile(typeof (DynamicAttribute).GetTypeInfo().Assembly.Location),
                MetadataReference.CreateFromFile(Assembly.Load(new AssemblyName("System.Runtime")).Location),
                MetadataReference.CreateFromFile(Assembly.Load(new AssemblyName("Microsoft.CSharp")).Location),
            };

        private static readonly SyntaxTree BaseSyntaxTree = CSharpSyntaxTree.ParseText(@"

using System;
using System.Linq;
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
        public StaticIndexBase Compile(IndexDefinition definition)
        {
            var cSharpSafeName = GetCSharpSafeName(definition);
            var syntaxNode = new TrnasformIndexClass(cSharpSafeName, definition).Visit(BaseSyntaxTree.GetRoot());

            var syntaxTree = SyntaxFactory.SyntaxTree(syntaxNode.NormalizeWhitespace());


            CSharpCompilation compilation = CSharpCompilation.Create(
                assemblyName: definition.Name + ".index.dll",
                syntaxTrees: new[] { syntaxTree },
                references: References,
                options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
                    .WithOptimizationLevel(OptimizationLevel.Release)
                );

            var asm = new MemoryStream();
            var pdb = new MemoryStream();

            var code = syntaxTree.ToString();

            var result = compilation.Emit(asm, pdb);

            if (result.Success == false)
            {
                IEnumerable<Diagnostic> failures = result.Diagnostics.Where(diagnostic =>
                 diagnostic.IsWarningAsError ||
                 diagnostic.Severity == DiagnosticSeverity.Error);

                var sb = new StringBuilder();
                sb.AppendLine($"Failed to compile index {definition.Name}");
                sb.AppendLine();
                sb.AppendLine(code);
                sb.AppendLine();
                foreach (Diagnostic diagnostic in failures)
                {
                    sb.AppendLine(diagnostic.ToString());
                }

                throw new IndexCompilationException(sb.ToString());
            }
            asm.Position = 0;
            pdb.Position = 0;
            var indexAssembly = AssemblyLoadContext.Default.LoadFromStream(asm, pdb);

            var type = indexAssembly.GetType("Raven.Server.Documents.Indexes.Static.Generated." + cSharpSafeName);

            var index = (StaticIndexBase)Activator.CreateInstance(type);

            index.Definition = definition;
            index.Source = code;

            return index;
        }


        private string GetCSharpSafeName(IndexDefinition definition)
        {
            return $"Index_{Regex.Replace(definition.Name, @"[^\w\d]", "_")}";
        }
    }

    internal class TrnasformIndexClass : CSharpSyntaxRewriter
    {
        private readonly IndexDefinition _definition;
        private readonly SyntaxToken _name;

        public TrnasformIndexClass(string name, IndexDefinition definition)
        {
            _definition = definition;
            _name = SyntaxFactory.Identifier(name);
        }

        public override SyntaxNode VisitClassDeclaration(ClassDeclarationSyntax node)
        {
            return base.VisitClassDeclaration(node.WithIdentifier(_name));
        }

        public override SyntaxNode VisitConstructorDeclaration(ConstructorDeclarationSyntax node)
        {
            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var map in _definition.Maps)
            {
                var invocationExpressionSyntax = HandleMap(map);
                node = node.AddBodyStatements(SyntaxFactory.ExpressionStatement(invocationExpressionSyntax));
            }

            return node.WithIdentifier(_name);
        }
        private InvocationExpressionSyntax HandleMap(string map)
        {
            try
            {
                StatementSyntax statement = SyntaxFactory.ParseStatement("var q = " + map);
                var declaration = (LocalDeclarationStatementSyntax)statement.SyntaxTree.GetRoot();
                var expression = declaration.Declaration.Variables[0].Initializer.Value;

                var queryExpression = expression as QueryExpressionSyntax;
                if (queryExpression == null)
                    throw new InvalidOperationException("A map clause must be a valid query");
                var mapRewriter = new MapRewriter();
                queryExpression = (QueryExpressionSyntax)mapRewriter.Visit(queryExpression);
                var arguments = SyntaxFactory.SeparatedList<ArgumentSyntax>(new SyntaxNodeOrToken[]
                {
                    // "Users" or null, the collection name
                    SyntaxFactory.Argument(SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, SyntaxFactory.Literal(mapRewriter.CollectionName))),
                    SyntaxFactory.Token(SyntaxKind.CommaToken),
                    // docs => from doc in docs ...
                    SyntaxFactory.Argument(SyntaxFactory.SimpleLambdaExpression(SyntaxFactory.Parameter(SyntaxFactory.Identifier("docs")), queryExpression)),

                });

                return SyntaxFactory.InvocationExpression(
                    SyntaxFactory.IdentifierName("AddMap"))
                    .WithArgumentList(SyntaxFactory.ArgumentList(arguments));


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

    }

    internal class MapRewriter : CSharpSyntaxRewriter
    {
        public string CollectionName;

        public override SyntaxNode VisitFromClause(FromClauseSyntax node)
        {
            if (CollectionName != null)
                return node;

            var docsExpression = node.Expression as MemberAccessExpressionSyntax;
            if (docsExpression == null)
                return node;

            var docsIdentifier = docsExpression.Expression as IdentifierNameSyntax;
            if (string.Equals(docsIdentifier?.Identifier.Text, "docs", StringComparison.OrdinalIgnoreCase) == false)
                return node;

            CollectionName = docsExpression.Name.Identifier.Text;

            return node.WithExpression(docsExpression.Expression);
        }

    }
}