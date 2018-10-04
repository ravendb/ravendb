using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.Loader;
using System.Text;
using System.Text.RegularExpressions;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Emit;
using Microsoft.CodeAnalysis.Formatting;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Compilation;
using Raven.Server.Documents.Indexes.Static.Roslyn;
using Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters;
using Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters.ReduceIndex;

namespace Raven.Server.Documents.Indexes.Static
{
    [SuppressMessage("ReSharper", "ConditionIsAlwaysTrueOrFalse")]
    public static class IndexCompiler
    {
        internal static readonly bool EnableDebugging = false; // for debugging purposes (mind http://issues.hibernatingrhinos.com/issue/RavenDB-6960)

        private const string IndexNamespace = "Raven.Server.Documents.Indexes.Static.Generated";

        private const string IndexExtension = ".index";

        private static readonly Lazy<(string Path, AssemblyName AssemblyName, MetadataReference Reference)[]> KnownManagedDlls = new Lazy<(string, AssemblyName, MetadataReference)[]>(DiscoverManagedDlls);

        static IndexCompiler()
        {
            AssemblyLoadContext.Default.Resolving += (ctx, name) =>
            {
                if (KnownManagedDlls.IsValueCreated == false)
                    return null; // this also handles the case of failure

                foreach (var item in KnownManagedDlls.Value)
                {
                    if (name.FullName == item.AssemblyName.FullName)
                        return Assembly.LoadFile(item.Path);
                }
                return null;
            };
        }


        private static (string Path, AssemblyName AssemblyName, MetadataReference Reference)[] DiscoverManagedDlls()
        {
            var path = Path.GetDirectoryName(typeof(IndexCompiler).GetTypeInfo().Assembly.Location);
            var results = new List<(string, AssemblyName, MetadataReference)>();

            foreach (var dll in Directory.GetFiles(path, "*.dll"))
            {
                AssemblyName name;
                try
                {
                    name = AssemblyLoadContext.GetAssemblyName(dll);
                }
                catch (Exception)
                {
                    continue; // we have unmanaged dlls (libsodium) here
                }
                var reference = MetadataReference.CreateFromFile(dll);
                results.Add((dll, name, reference));
            }
            return results.ToArray();
        }

        private static readonly UsingDirectiveSyntax[] Usings =
        {
            SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName("System")),
            SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName("System.Collections")),
            SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName("System.Collections.Generic")),
            SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName("System.Globalization")),
            SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName("System.Linq")),
            SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName("System.Text.RegularExpressions")),

            SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName("Lucene.Net.Documents")),

            SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName(typeof(CreateFieldOptions).Namespace)),

            SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName("Raven.Server.Documents.Indexes.Static")),
            SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName("Raven.Server.Documents.Indexes.Static.Linq")),
            SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName("Raven.Server.Documents.Indexes.Static.Extensions"))
        };

        private static readonly MetadataReference[] References =
        {
            MetadataReference.CreateFromFile(typeof(object).GetTypeInfo().Assembly.Location),
            MetadataReference.CreateFromFile(typeof(ExpressionType).GetTypeInfo().Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Enumerable).GetTypeInfo().Assembly.Location),
            MetadataReference.CreateFromFile(typeof(IndexCompiler).GetTypeInfo().Assembly.Location),
            MetadataReference.CreateFromFile(typeof(BoostedValue).GetTypeInfo().Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Lucene.Net.Documents.Document).GetTypeInfo().Assembly.Location),
            MetadataReference.CreateFromFile(Assembly.Load(new AssemblyName("System.Runtime")).Location),
            MetadataReference.CreateFromFile(Assembly.Load(new AssemblyName("Microsoft.CSharp")).Location),
            MetadataReference.CreateFromFile(Assembly.Load(new AssemblyName("mscorlib")).Location),
            MetadataReference.CreateFromFile(Assembly.Load(new AssemblyName("netstandard")).Location),
            MetadataReference.CreateFromFile(Assembly.Load(new AssemblyName("System.Collections")).Location),
            MetadataReference.CreateFromFile(typeof(Regex).GetTypeInfo().Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Uri).GetTypeInfo().Assembly.Location)
        };


        public static StaticIndexBase Compile(IndexDefinition definition)
        {
            var cSharpSafeName = GetCSharpSafeName(definition.Name);

            var @class = CreateClass(cSharpSafeName, definition);

            var compilationResult = CompileInternal(definition.Name, cSharpSafeName, @class, extensions: definition.AdditionalSources);
            var type = compilationResult.Type;

            var index = (StaticIndexBase)Activator.CreateInstance(type);
            index.Source = compilationResult.Code;

            return index;
        }

        private static CompilationResult CompileInternal(string originalName, string cSharpSafeName, MemberDeclarationSyntax @class, Dictionary<string, string> extensions = null)
        {
            var name = cSharpSafeName + "." + Guid.NewGuid() + IndexExtension;

            var @namespace = RoslynHelper.CreateNamespace(IndexNamespace)
                .WithMembers(SyntaxFactory.SingletonList(@class));

            var res = GetUsingDirectiveAndSyntaxTreesAndReferences(extensions);

            var compilationUnit = SyntaxFactory.CompilationUnit()
                .WithUsings(RoslynHelper.CreateUsings(res.UsingDirectiveSyntaxes))
                .WithMembers(SyntaxFactory.SingletonList<MemberDeclarationSyntax>(@namespace))
                .NormalizeWhitespace();

            SyntaxNode formattedCompilationUnit;
            using (var workspace = new AdhocWorkspace())
            {
                formattedCompilationUnit = Formatter.Format(compilationUnit, workspace);
            }

            string sourceFile = null;

            if (EnableDebugging)
            {
                sourceFile = Path.Combine(Path.GetTempPath(), name + ".cs");
                File.WriteAllText(sourceFile, formattedCompilationUnit.ToFullString(), Encoding.UTF8);
            }

            var st = EnableDebugging
                ? SyntaxFactory.ParseSyntaxTree(File.ReadAllText(sourceFile), path: sourceFile, encoding: Encoding.UTF8)
                : SyntaxFactory.ParseSyntaxTree(formattedCompilationUnit.ToFullString());

            res.SyntaxTrees.Add(st);
            var syntaxTrees = res.SyntaxTrees;

            var compilation = CSharpCompilation.Create(
                assemblyName: name + ".dll",
                syntaxTrees: syntaxTrees,
                references: res.References,
                options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
                    .WithOptimizationLevel(EnableDebugging ? OptimizationLevel.Debug : OptimizationLevel.Release)
                );

            var code = formattedCompilationUnit.SyntaxTree.ToString();

            var asm = new MemoryStream();
            var pdb = EnableDebugging ? new MemoryStream() : null;

            var result = compilation.Emit(asm, pdb, options: new EmitOptions(debugInformationFormat: DebugInformationFormat.PortablePdb));

            if (result.Success == false)
            {
                IEnumerable<Diagnostic> failures = result.Diagnostics
                    .Where(diagnostic => diagnostic.IsWarningAsError || diagnostic.Severity == DiagnosticSeverity.Error);

                var sb = new StringBuilder();
                sb.AppendLine($"Failed to compile index {originalName}");
                sb.AppendLine();
                sb.AppendLine(code);
                sb.AppendLine();

                foreach (var diagnostic in failures)
                    sb.AppendLine(diagnostic.ToString());


                throw new IndexCompilationException(sb.ToString());

            }

            asm.Position = 0;

            Assembly assembly;

            if (EnableDebugging)
            {
                pdb.Position = 0;
                assembly = AssemblyLoadContext.Default.LoadFromStream(asm, pdb);
            }
            else
            {
                assembly = AssemblyLoadContext.Default.LoadFromStream(asm);
            }

            return new CompilationResult
            {
                Code = code,
                Type = assembly.GetType($"{IndexNamespace}.{cSharpSafeName}")
            };
        }

        private static (UsingDirectiveSyntax[] UsingDirectiveSyntaxes, List<SyntaxTree> SyntaxTrees, MetadataReference[] References) GetUsingDirectiveAndSyntaxTreesAndReferences(Dictionary<string, string> extensions)
        {
            var syntaxTrees = new List<SyntaxTree>();
            if (extensions == null)
            {
                return (Usings, syntaxTrees, References);
            }
            var @using = new HashSet<string>();

            foreach (var ext in extensions)
            {
                var rewrite = MethodDynamicParametersRewriter.Instance.Visit(SyntaxFactory.ParseSyntaxTree(ext.Value).GetRoot());
                var tree = SyntaxFactory.SyntaxTree(rewrite);
                syntaxTrees.Add(tree);
                var ns = rewrite.DescendantNodes().OfType<NamespaceDeclarationSyntax>().FirstOrDefault();
                if (ns != null)
                {
                    @using.Add(ns.Name.ToString());
                }
            }
            var references = GetReferences();
            if (@using.Count > 0)
            {
                //Adding using directive with duplicates to avoid O(n*m) operation and confusing code
                var newUsing = @using.Select(x => SyntaxFactory.UsingDirective(SyntaxFactory.ParseName(x))).ToList();
                newUsing.AddRange(Usings);
                return (newUsing.ToArray(), syntaxTrees, references);
            }
            return (Usings, syntaxTrees, references);
        }

        private static MetadataReference[] GetReferences()
        {
            var knownManageRefs = KnownManagedDlls.Value;
            var newReferences = new MetadataReference[References.Length + knownManageRefs.Length];
            for (var i = 0; i < References.Length; i++)
            {
                newReferences[i] = References[i];
            }
            for (int i = 0; i < knownManageRefs.Length; i++)
            {
                newReferences[i + References.Length] = knownManageRefs[i].Reference;
            }
            return newReferences;
        }

        private static MemberDeclarationSyntax CreateClass(string name, IndexDefinition definition)
        {
            var statements = new List<StatementSyntax>();
            var maps = definition.Maps.ToList();
            var fieldNamesValidator = new FieldNamesValidator();
            var methodDetector = new MethodDetectorRewriter();
            var members = new SyntaxList<MemberDeclarationSyntax>();

            for (var i = 0; i < maps.Count; i++)
            {
                var map = maps[i];
                statements.AddRange(HandleMap(map, fieldNamesValidator, methodDetector, ref members));
            }

            if (string.IsNullOrWhiteSpace(definition.Reduce) == false)
            {
                statements.Add(HandleReduce(definition.Reduce, fieldNamesValidator, methodDetector, out CompiledIndexField[] groupByFields));

                var groupByFieldsArray = GetArrayCreationExpression<CompiledIndexField>(
                    groupByFields,
                    (builder, field) => field.WriteTo(builder));

                statements.Add(RoslynHelper.This(nameof(StaticIndexBase.GroupByFields)).Assign(groupByFieldsArray).AsExpressionStatement());
            }

            var fields = GetIndexedFields(definition, fieldNamesValidator);

            var outputFieldsArray = GetArrayCreationExpression<string>(
                fields,
                (builder, field) => builder.Append("\"").Append(field.Name).Append("\""));

            statements.Add(RoslynHelper.This(nameof(StaticIndexBase.OutputFields)).Assign(outputFieldsArray).AsExpressionStatement());

            var methods = methodDetector.Methods;

            if (methods.HasCreateField)
                statements.Add(RoslynHelper.This(nameof(StaticIndexBase.HasDynamicFields)).Assign(SyntaxFactory.LiteralExpression(SyntaxKind.TrueLiteralExpression)).AsExpressionStatement());

            if (methods.HasBoost)
                statements.Add(RoslynHelper.This(nameof(StaticIndexBase.HasBoostedFields)).Assign(SyntaxFactory.LiteralExpression(SyntaxKind.TrueLiteralExpression)).AsExpressionStatement());

            var ctor = RoslynHelper.PublicCtor(name)
                .AddBodyStatements(statements.ToArray());


            return RoslynHelper.PublicClass(name)
                .WithBaseClass<StaticIndexBase>()
                .WithMembers(members.Add(ctor));
        }

        private static List<CompiledIndexField> GetIndexedFields(IndexDefinition definition, FieldNamesValidator fieldNamesValidator)
        {
            var fields = fieldNamesValidator.Fields.ToList();

            foreach (var spatialField in definition.Fields)
            {
                if (spatialField.Value.Spatial == null)
                    continue;
                if (spatialField.Value.Spatial.Strategy != Client.Documents.Indexes.Spatial.SpatialSearchStrategy.BoundingBox)
                    continue;

                fields.Remove(new SimpleField(spatialField.Key));
                fields.AddRange(new[]
                {
                    new SimpleField(spatialField.Key + "__minX"),
                    new SimpleField(spatialField.Key + "__minY"),
                    new SimpleField(spatialField.Key + "__maxX"),
                    new SimpleField(spatialField.Key + "__maxY")
                });
            }

            return fields;
        }

        private static List<StatementSyntax> HandleMap(string map, FieldNamesValidator fieldNamesValidator, MethodDetectorRewriter methodsDetector,
            ref SyntaxList<MemberDeclarationSyntax> members)
        {
            try
            {
                map = NormalizeFunction(map);
                var expression = SyntaxFactory.ParseExpression(map).NormalizeWhitespace();

                fieldNamesValidator.Validate(map, expression);
                methodsDetector.Visit(expression);

                var queryExpression = expression as QueryExpressionSyntax;
                if (queryExpression != null)
                    return HandleSyntaxInMap(fieldNamesValidator, new MapFunctionProcessor(CollectionNameRetriever.QuerySyntax, SelectManyRewriter.QuerySyntax), queryExpression, ref members);
                var invocationExpression = expression as InvocationExpressionSyntax;
                if (invocationExpression != null)
                    return HandleSyntaxInMap(fieldNamesValidator, new MapFunctionProcessor(CollectionNameRetriever.MethodSyntax, SelectManyRewriter.MethodSyntax), invocationExpression, ref members);

                throw new InvalidOperationException("Not supported expression type.");
            }
            catch (Exception ex)
            {
                throw new IndexCompilationException(ex.Message, ex)
                {
                    IndexDefinitionProperty = nameof(IndexDefinition.Maps),
                    ProblematicText = map
                };
            }
        }

        private static StatementSyntax HandleReduce(string reduce, FieldNamesValidator fieldNamesValidator, MethodDetectorRewriter methodsDetector, out CompiledIndexField[] groupByFields)
        {
            try
            {
                reduce = NormalizeFunction(reduce);
                var expression = SyntaxFactory.ParseExpression(reduce).NormalizeWhitespace();
                fieldNamesValidator?.Validate(reduce, expression);
                methodsDetector.Visit(expression);

                StatementSyntax result;

                switch (expression)
                {
                    case QueryExpressionSyntax queryExpression:
                        result = HandleSyntaxInReduce(
                            new ReduceFunctionProcessor(
                                ResultsVariableNameRewriter.QuerySyntax,
                                GroupByFieldsRetriever.QuerySyntax,
                                SelectManyRewriter.QuerySyntax),
                            MethodsInGroupByValidator.QuerySyntaxValidator,
                            queryExpression, out groupByFields);
                        break;
                    case InvocationExpressionSyntax invocationExpression:
                        result = HandleSyntaxInReduce(
                            new ReduceFunctionProcessor(
                                ResultsVariableNameRewriter.MethodSyntax,
                                GroupByFieldsRetriever.MethodSyntax,
                                SelectManyRewriter.MethodSyntax),
                            MethodsInGroupByValidator.MethodSyntaxValidator,
                            invocationExpression, out groupByFields);
                        break;
                    default:
                        throw new InvalidOperationException("Not supported expression type.");
                }

                if (groupByFields == null)
                {
                    throw new InvalidOperationException("Reduce function must contain a group by expression.");
                }

                foreach (var groupByField in groupByFields)
                {
                    if (fieldNamesValidator?.Fields.Contains(groupByField) == false)
                    {
                        throw new InvalidOperationException($"Group by field '{groupByField.Name}' was not found on the list of index fields ({string.Join(", ",fieldNamesValidator.Fields.Select(x => x.Name))})");
                    }
                }

                return result;
            }
            catch (Exception ex)
            {
                throw new IndexCompilationException(ex.Message, ex)
                {
                    IndexDefinitionProperty = nameof(IndexDefinition.Reduce),
                    ProblematicText = reduce
                };
            }
        }


        private static List<StatementSyntax> HandleSyntaxInMap(FieldNamesValidator fieldValidator, MapFunctionProcessor mapRewriter, ExpressionSyntax expression,
            ref SyntaxList<MemberDeclarationSyntax> members)
        {
            var rewrittenExpression = (CSharpSyntaxNode)mapRewriter.Visit(expression);

            StatementSyntax optimized = null;

            try
            {
                var visitor = new RavenLinqOptimizer(fieldValidator);
                optimized = visitor.Visit(new RavenLinqPrettifier().Visit(rewrittenExpression)) as StatementSyntax;
            }
            catch (NotSupportedException)
            {
                // there are certain patterns we aren't optimizing, that is fine
            }
            var collectionName = string.IsNullOrWhiteSpace(mapRewriter.CollectionName) ? Constants.Documents.Collections.AllDocumentsCollection : mapRewriter.CollectionName;

            var collection = SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, SyntaxFactory.Literal(collectionName));
            var results = new List<StatementSyntax>();

            if (optimized != null)
            {
                var method = SyntaxFactory.MethodDeclaration(SyntaxFactory.IdentifierName("IEnumerable"), SyntaxFactory.Identifier("Map_" + members.Count))
                    .WithParameterList(
                        SyntaxFactory.ParameterList(SyntaxFactory.SingletonSeparatedList(
                            SyntaxFactory.Parameter(SyntaxFactory.Identifier("docs"))
                                .WithType(
                                    SyntaxFactory.GenericName("IEnumerable")
                                    .WithTypeArgumentList(
                                        SyntaxFactory.TypeArgumentList(
                                            SyntaxFactory.SingletonSeparatedList<TypeSyntax>(SyntaxFactory.IdentifierName("dynamic"))
                                            )
                                        )
                                )
                            ))
                    )
                    .WithBody(SyntaxFactory.Block().AddStatements(optimized));

                members = members.Add(method);

                results.Add(RoslynHelper.This(nameof(StaticIndexBase.AddMap)).Invoke(collection, RoslynHelper.This(method.Identifier.Text)).AsExpressionStatement()); // this.AddMap("Users", docs => from doc in docs ... )
            }
            else
            {
                var indexingFunction = SyntaxFactory.SimpleLambdaExpression(SyntaxFactory.Parameter(SyntaxFactory.Identifier("docs")), rewrittenExpression);

                results.Add(RoslynHelper.This(nameof(StaticIndexBase.AddMap)).Invoke(collection, indexingFunction).AsExpressionStatement()); // this.AddMap("Users", docs => from doc in docs ... )
            }

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

        private static StatementSyntax HandleSyntaxInReduce(ReduceFunctionProcessor reduceFunctionProcessor, MethodsInGroupByValidator methodsInGroupByValidator,
            ExpressionSyntax expression, out CompiledIndexField[] groupByFields)
        {

            var rewrittenExpression = (CSharpSyntaxNode)reduceFunctionProcessor.Visit(expression);

            var reducingFunction =
                SyntaxFactory.SimpleLambdaExpression(
                    SyntaxFactory.Parameter(SyntaxFactory.Identifier(ResultsVariableNameRewriter.ResultsVariable)),
                    rewrittenExpression);

            methodsInGroupByValidator.Start(expression);

            groupByFields = reduceFunctionProcessor.GroupByFields;

            return RoslynHelper.This(nameof(StaticIndexBase.Reduce)).Assign(reducingFunction).AsExpressionStatement();
        }

        private static ArrayCreationExpressionSyntax GetArrayCreationExpression<T>(IEnumerable<CompiledIndexField> items, Action<StringBuilder, CompiledIndexField> write)
        {
            var sb = new StringBuilder();
            sb.Append("new ");
            sb.Append(typeof(T).FullName);
            sb.Append("[] {");
            var first = true;
            foreach (var item in items)
            {
                if (first == false)
                    sb.Append(",");

                first = false;

                write(sb, item);
            }
            sb.Append("}");

            return (ArrayCreationExpressionSyntax)SyntaxFactory.ParseExpression(sb.ToString());
        }

        private static string GetCSharpSafeName(string name)
        {
            return $"Index_{Regex.Replace(name, @"[^\w\d]", "_")}";
        }

        private static string NormalizeFunction(string function)
        {
            return function?.Trim().TrimEnd(';');
        }

        private class CompilationResult
        {
            public Type Type { get; set; }
            public string Code { get; set; }
        }

        public class IndexMethods
        {
            public bool HasLoadDocument { get; set; }

            public bool HasTransformWith { get; set; }

            public bool HasGroupBy { get; set; }

            public bool HasInclude { get; set; }

            public bool HasCreateField { get; set; }

            public bool HasBoost { get; set; }
        }
    }
}
