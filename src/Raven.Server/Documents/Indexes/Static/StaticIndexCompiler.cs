using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.Loader;
using System.Text;
using System.Text.RegularExpressions;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

using Raven.Abstractions.Exceptions;
using Raven.Client.Indexing;

namespace Raven.Server.Documents.Indexes.Static
{
    public static class StaticIndexCompiler
    {
        private static readonly UsingDirectiveSyntax[] Usings =
        {
            SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName("System")),
            SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName("System.Linq")),
            SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName("Raven.Server.Documents.Indexes.Static"))
        };

        private static readonly MetadataReference[] References =
        {
            MetadataReference.CreateFromFile(typeof (object).GetTypeInfo().Assembly.Location),
            MetadataReference.CreateFromFile(typeof (Enumerable).GetTypeInfo().Assembly.Location),
            MetadataReference.CreateFromFile(typeof (StaticIndexCompiler).GetTypeInfo().Assembly.Location),
            MetadataReference.CreateFromFile(typeof (DynamicAttribute).GetTypeInfo().Assembly.Location),
            MetadataReference.CreateFromFile(Assembly.Load(new AssemblyName("System.Runtime")).Location),
            MetadataReference.CreateFromFile(Assembly.Load(new AssemblyName("Microsoft.CSharp")).Location),
        };

        public static StaticIndexBase Compile(IndexDefinition definition)
        {
            var cSharpSafeName = GetCSharpSafeName(definition);

            var @class = CreateClass(cSharpSafeName, definition);

            var @namespace = RoslynHelper.CreateNamespace("Raven.Server.Documents.Indexes.Static.Generated")
                .WithMembers(SyntaxFactory.SingletonList(@class));

            var compilationUnit = SyntaxFactory.CompilationUnit()
                .WithUsings(RoslynHelper.CreateUsings(Usings))
                .WithMembers(SyntaxFactory.SingletonList<MemberDeclarationSyntax>(@namespace))
                .NormalizeWhitespace();

            var formatedCompilationUnit = compilationUnit; //Formatter.Format(compilationUnit, new AdhocWorkspace());

            var compilation = CSharpCompilation.Create(
                assemblyName: definition.Name + ".index.dll",
                syntaxTrees: new[] { SyntaxFactory.ParseSyntaxTree(formatedCompilationUnit.ToFullString()) }, // TODO [ppekrol] for some reason formatedCompilationUnit.SyntaxTree does not work
                references: References,
                options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
                    .WithOptimizationLevel(OptimizationLevel.Release)
                );

            var code = formatedCompilationUnit.SyntaxTree.ToString();

            var asm = new MemoryStream();
            //var pdb = new MemoryStream();

            var result = compilation.Emit(asm);

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

                foreach (var diagnostic in failures)
                    sb.AppendLine(diagnostic.ToString());

                throw new IndexCompilationException(sb.ToString());
            }

            asm.Position = 0;
            //pdb.Position = 0;
            //var indexAssembly = AssemblyLoadContext.Default.LoadFromStream(asm, pdb);
            var indexAssembly = AssemblyLoadContext.Default.LoadFromStream(asm);

            var type = indexAssembly.GetType("Raven.Server.Documents.Indexes.Static.Generated." + cSharpSafeName);

            var index = (StaticIndexBase)Activator.CreateInstance(type);
            index.Source = code;

            return index;
        }

        private static MemberDeclarationSyntax CreateClass(string name, IndexDefinition definition)
        {
            var statements = new List<StatementSyntax>();
            statements.AddRange(definition.Maps.Select(map => HandleMap(map, definition)));

            //if (string.IsNullOrWhiteSpace(definition.Reduce) == false)
            //    statements.Add(HandleReduceFunction(definition));

            var ctor = RoslynHelper.PublicCtor(name)
                .AddBodyStatements(statements.ToArray());

            return RoslynHelper.PublicClass(name)
                .WithBaseClass<StaticIndexBase>()
                .WithMembers(SyntaxFactory.SingletonList<MemberDeclarationSyntax>(ctor));
        }

        private static StatementSyntax HandleMap(string map, IndexDefinition definition)
        {
            try
            {
                var expression = SyntaxFactory.ParseExpression(map);
                var queryExpression = expression as QueryExpressionSyntax;
                if (queryExpression == null)
                    throw new InvalidOperationException();

                var mapRewriter = new MapRewriter();
                queryExpression = (QueryExpressionSyntax)mapRewriter.Visit(queryExpression);

                var collectionName = mapRewriter.CollectionName;
                var collection = SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, SyntaxFactory.Literal(collectionName));
                var indexingFunction = SyntaxFactory.SimpleLambdaExpression(SyntaxFactory.Parameter(SyntaxFactory.Identifier("docs")), queryExpression);

                return RoslynHelper.This("AddMap") // this.AddMap("Users", docs => from doc in docs ... )
                    .Invoke(collection, indexingFunction)
                    .AsExpressionStatement();
            }
            catch (Exception ex)
            {
                throw new IndexCompilationException(ex.Message, ex)
                {
                    IndexDefinitionProperty = "Maps",
                    ProblematicText = map
                };
            }
        }

        private static string GetCSharpSafeName(IndexDefinition definition)
        {
            return $"Index_{Regex.Replace(definition.Name, @"[^\w\d]", "_")}";
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