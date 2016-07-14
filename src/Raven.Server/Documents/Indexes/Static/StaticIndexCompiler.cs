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
using Raven.Client.Data;
using Raven.Client.Exceptions;
using Raven.Client.Indexing;
using Raven.Server.Documents.Indexes.Static.Roslyn;
using Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters.ReduceIndex;

namespace Raven.Server.Documents.Indexes.Static
{
    public static class StaticIndexCompiler
    {
        private static readonly UsingDirectiveSyntax[] Usings =
        {
            SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName("System")),
            SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName("System.Collections.Generic")),
            SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName("System.Linq")),
            SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName("Raven.Server.Documents.Indexes.Static")),
            SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName("Raven.Server.Documents.Indexes.Static.Linq")),
        };

        private static readonly MetadataReference[] References =
        {
            MetadataReference.CreateFromFile(typeof(object).GetTypeInfo().Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Enumerable).GetTypeInfo().Assembly.Location),
            MetadataReference.CreateFromFile(typeof(StaticIndexCompiler).GetTypeInfo().Assembly.Location),
            MetadataReference.CreateFromFile(typeof(DynamicAttribute).GetTypeInfo().Assembly.Location),
            MetadataReference.CreateFromFile(typeof(BoostedValue).GetTypeInfo().Assembly.Location),
            MetadataReference.CreateFromFile(Assembly.Load(new AssemblyName("System.Runtime")).Location),
            MetadataReference.CreateFromFile(Assembly.Load(new AssemblyName("Microsoft.CSharp")).Location),
            MetadataReference.CreateFromFile(Assembly.Load(new AssemblyName("mscorlib")).Location),
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
                assemblyName: cSharpSafeName + "." + Guid.NewGuid() + ".index.dll",
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
            var maps = definition.Maps.ToList();
            var fieldNamesValidator = new FieldNamesValidator();
            for (var i = 0; i < maps.Count; i++)
            {
                var map = maps[i];
                statements.AddRange(HandleMap(map, fieldNamesValidator));
            }

            if (string.IsNullOrWhiteSpace(definition.Reduce) == false)
                statements.Add(HandleReduce(definition.Reduce, fieldNamesValidator));

            var ctor = RoslynHelper.PublicCtor(name)
                .AddBodyStatements(statements.ToArray());

            return RoslynHelper.PublicClass(name)
                .WithBaseClass<StaticIndexBase>()
                .WithMembers(SyntaxFactory.SingletonList<MemberDeclarationSyntax>(ctor));
        }

        private static List<StatementSyntax> HandleMap(string map, FieldNamesValidator fieldNamesValidator)
        {
            try
            {
                var expression = SyntaxFactory.ParseExpression(map);

                fieldNamesValidator.Validate(map, expression);

                var queryExpression = expression as QueryExpressionSyntax;
                if (queryExpression != null)
                    return HandleSyntaxInMap(new QuerySyntaxMapRewriter(), queryExpression);

                var invocationExpression = expression as InvocationExpressionSyntax;
                if (invocationExpression != null)
                    return HandleSyntaxInMap(new MethodSyntaxMapRewriter(), invocationExpression);

                throw new InvalidOperationException("Not supported expression type.");
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

        private static StatementSyntax HandleReduce(string reduce, FieldNamesValidator fieldNamesValidator)
        {
            try
            {
                var expression = SyntaxFactory.ParseExpression(reduce);

                fieldNamesValidator.Validate(reduce, expression);

                var queryExpression = expression as QueryExpressionSyntax;
                if (queryExpression != null)
                    return HandleSyntaxInReduce(new ReduceFunctionProcessor(ResultsVariableNameRetriever.QuerySyntax, GroupByFieldsRetriever.QuerySyntax), queryExpression);

                var invocationExpression = expression as InvocationExpressionSyntax;
                if (invocationExpression != null)
                    return HandleSyntaxInReduce(new ReduceFunctionProcessor(ResultsVariableNameRetriever.MethodSyntax, GroupByFieldsRetriever.MethodSyntax), invocationExpression);

                throw new InvalidOperationException("Not supported expression type.");
            }
            catch (Exception ex)
            {
                throw new IndexCompilationException(ex.Message, ex)
                {
                    IndexDefinitionProperty = "Reduce",
                    ProblematicText = reduce
                };
            }
        }

        private static List<StatementSyntax> HandleSyntaxInMap(MapRewriterBase mapRewriter, ExpressionSyntax expression)
        {
            var rewrittenExpression = (CSharpSyntaxNode)mapRewriter.Visit(expression);
            if (string.IsNullOrWhiteSpace(mapRewriter.CollectionName))
                throw new InvalidOperationException("Could not extract collection name from expression");

            var collection = SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, SyntaxFactory.Literal(mapRewriter.CollectionName));
            var indexingFunction = SyntaxFactory.SimpleLambdaExpression(SyntaxFactory.Parameter(SyntaxFactory.Identifier("docs")), rewrittenExpression);

            var results = new List<StatementSyntax>();
            results.Add(RoslynHelper.This(nameof(StaticIndexBase.AddMap)).Invoke(collection, indexingFunction).AsExpressionStatement()); // this.AddMap("Users", docs => from doc in docs ... )

            if (mapRewriter.ReferencedCollections != null)
            {
                foreach (var referencedCollection in mapRewriter.ReferencedCollections)
                {
                    var rc = SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, SyntaxFactory.Literal(referencedCollection));
                    results.Add(RoslynHelper.This(nameof(StaticIndexBase.AddReferencedCollection)).Invoke(collection, rc).AsExpressionStatement());
                }
            }

            return results;
        }

        private static StatementSyntax HandleSyntaxInReduce(ReduceFunctionProcessor reduceFunctionProcessor, ExpressionSyntax expression)
        {
            var rewrittenExpression = (CSharpSyntaxNode)reduceFunctionProcessor.Visit(expression);

            var indexingFunction = SyntaxFactory.SimpleLambdaExpression(SyntaxFactory.Parameter(SyntaxFactory.Identifier(reduceFunctionProcessor.ResultsVariableName)), rewrittenExpression);

            var groupByFields = SyntaxFactory.ArrayCreationExpression(SyntaxFactory.ArrayType(
                        SyntaxFactory.PredefinedType(SyntaxFactory.Token(SyntaxKind.StringKeyword)))
                    .WithRankSpecifiers(
                        SyntaxFactory.SingletonList<ArrayRankSpecifierSyntax>(
                            SyntaxFactory.ArrayRankSpecifier(
                                    SyntaxFactory.SingletonSeparatedList<ExpressionSyntax>(
                                        SyntaxFactory.OmittedArraySizeExpression()
                                            .WithOmittedArraySizeExpressionToken(
                                                SyntaxFactory.Token(SyntaxKind.OmittedArraySizeExpressionToken))))
                                .WithOpenBracketToken(SyntaxFactory.Token(SyntaxKind.OpenBracketToken))
                                .WithCloseBracketToken(SyntaxFactory.Token(SyntaxKind.CloseBracketToken)))))
                .WithNewKeyword(SyntaxFactory.Token(SyntaxKind.NewKeyword))
                .WithInitializer(SyntaxFactory.InitializerExpression(SyntaxKind.ArrayInitializerExpression,
                        SyntaxFactory.SeparatedList<ExpressionSyntax>(reduceFunctionProcessor.GroupByFields.Select(
                            x =>
                                SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression,
                                    SyntaxFactory.Literal(x)))))
                    .WithOpenBraceToken(SyntaxFactory.Token(SyntaxKind.OpenBraceToken))
                    .WithCloseBraceToken(SyntaxFactory.Token(SyntaxKind.CloseBraceToken)));

            return RoslynHelper.This("SetReduce")
                .Invoke(indexingFunction, groupByFields).AsExpressionStatement();
        }

        private static string GetCSharpSafeName(IndexDefinition definition)
        {
            return $"Index_{Regex.Replace(definition.Name, @"[^\w\d]", "_")}";
        }
    }
}