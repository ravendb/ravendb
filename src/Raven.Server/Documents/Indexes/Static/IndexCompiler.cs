using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Metadata;
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
using Raven.Client.Util;
using Raven.Server.Documents.Indexes.Static.NuGet;
using Raven.Server.Documents.Indexes.Static.Roslyn;
using Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters;
using Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters.Counters;
using Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters.ReduceIndex;
using Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters.TimeSeries;

namespace Raven.Server.Documents.Indexes.Static
{
    [SuppressMessage("ReSharper", "ConditionIsAlwaysTrueOrFalse")]
    public static class IndexCompiler
    {
        internal static readonly bool EnableDebugging = false; // for debugging purposes (mind https://issues.hibernatingrhinos.com/issue/RavenDB-6960)

        private const string IndexNamespace = "Raven.Server.Documents.Indexes.Static.Generated";

        internal const string IndexExtension = ".index";

        internal const string IndexNamePrefix = "Index_";

        private static readonly Lazy<ConcurrentDictionary<string, AdditionalAssemblyServerSide>> AdditionalAssemblies = new Lazy<ConcurrentDictionary<string, AdditionalAssemblyServerSide>>(DiscoverAdditionalAssemblies);

        static IndexCompiler()
        {
            AssemblyLoadContext.Default.Resolving += (ctx, name) =>
            {
                if (AdditionalAssemblies.IsValueCreated && AdditionalAssemblies.Value.TryGetValue(name.FullName, out var assembly))
                    return assembly.Assembly;

                return null;
            };
        }

        private static ConcurrentDictionary<string, AdditionalAssemblyServerSide> DiscoverAdditionalAssemblies()
        {
            var results = new ConcurrentDictionary<string, AdditionalAssemblyServerSide>(StringComparer.OrdinalIgnoreCase);

            foreach (var path in Directory.GetFiles(AppContext.BaseDirectory, "*.dll"))
            {
                try
                {
                    var name = AssemblyLoadContext.GetAssemblyName(path);
                    var assembly = Assembly.LoadFile(path);
                    var reference = CreateMetadataReferenceFromAssembly(assembly);

                    results.TryAdd(name.FullName, new AdditionalAssemblyServerSide(name, assembly, reference, AdditionalAssemblyType.BaseDirectory));
                }
                catch
                {
                    // we have unmanaged dlls (libsodium) here
                }
            }

            return results;
        }

        private static readonly string IndexesStaticNamespace = "Raven.Server.Documents.Indexes.Static";

        private static readonly UsingDirectiveSyntax[] Usings =
        {
            SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName("System")),
            SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName("System.Collections")),
            SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName("System.Collections.Generic")),
            SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName("System.Globalization")),
            SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName("System.Linq")),
            SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName("System.Text")),
            SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName("System.Text.RegularExpressions")),

            SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName("Lucene.Net.Documents")),

            SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName(typeof(CreateFieldOptions).Namespace)),

            SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName(IndexesStaticNamespace)),
            SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName("Raven.Server.Documents.Indexes.Static.Linq")),
            SyntaxFactory.UsingDirective(SyntaxFactory.IdentifierName("Raven.Server.Documents.Indexes.Static.Extensions"))
        };

        internal static readonly MetadataReference[] References =
        {
            CreateMetadataReferenceFromAssembly(typeof(object).Assembly),
            CreateMetadataReferenceFromAssembly(typeof(ExpressionType).Assembly),
            CreateMetadataReferenceFromAssembly(typeof(Enumerable).Assembly),
            CreateMetadataReferenceFromAssembly(typeof(IndexCompiler).Assembly),
            CreateMetadataReferenceFromAssembly(typeof(BoostedValue).Assembly),
            CreateMetadataReferenceFromAssembly(typeof(Lucene.Net.Documents.Document).Assembly),
            CreateMetadataReferenceFromAssembly(Assembly.Load(new AssemblyName("System.Runtime"))),
            CreateMetadataReferenceFromAssembly(Assembly.Load(new AssemblyName("Microsoft.CSharp"))),
            CreateMetadataReferenceFromAssembly(Assembly.Load(new AssemblyName("mscorlib"))),
            CreateMetadataReferenceFromAssembly(Assembly.Load(new AssemblyName("netstandard"))),
            CreateMetadataReferenceFromAssembly(Assembly.Load(new AssemblyName("System.Collections"))),
            CreateMetadataReferenceFromAssembly(typeof(Regex).Assembly),
            CreateMetadataReferenceFromAssembly(typeof(Uri).Assembly)
        };

        private unsafe static MetadataReference CreateMetadataReferenceFromAssembly(Assembly assembly)
        {
            assembly.TryGetRawMetadata(out var blob, out var length);
            var moduleMetadata = ModuleMetadata.CreateFromMetadata((IntPtr)blob, length);
            var assemblyMetadata = AssemblyMetadata.Create(moduleMetadata);
            return assemblyMetadata.GetReference();
        }

        public static AbstractStaticIndexBase Compile(IndexDefinition definition)
        {
            var cSharpSafeName = GetCSharpSafeName(definition.Name);

            var @class = CreateClass(cSharpSafeName, definition);

            var compilationResult = CompileInternal(definition.Name, cSharpSafeName, @class, definition);
            var type = compilationResult.Type;

            var index = (AbstractStaticIndexBase)Activator.CreateInstance(type);
            index.Source = compilationResult.Code;

            return index;
        }

        private static CompilationResult CompileInternal(string originalName, string cSharpSafeName, MemberDeclarationSyntax @class, IndexDefinition definition)
        {
            var name = cSharpSafeName + "." + Guid.NewGuid() + IndexExtension;

            var @namespace = RoslynHelper.CreateNamespace(IndexNamespace)
                .WithMembers(SyntaxFactory.SingletonList(@class));

            var res = GetUsingDirectiveAndSyntaxTreesAndReferences(definition);

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

            var compilation = CSharpCompilation.Create(
                assemblyName: name,
                syntaxTrees: res.SyntaxTrees,
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

        private static (UsingDirectiveSyntax[] UsingDirectiveSyntaxes, List<SyntaxTree> SyntaxTrees, MetadataReference[] References) GetUsingDirectiveAndSyntaxTreesAndReferences(IndexDefinition definition)
        {
            if (definition.AdditionalSources == null && definition.AdditionalSources == null)
            {
                return (Usings, new List<SyntaxTree>(), References);
            }

            (UsingDirectiveSyntax[] UsingDirectiveSyntaxes, List<SyntaxTree> SyntaxTrees, MetadataReference[] References) result;
            var syntaxTrees = new List<SyntaxTree>();
            var usings = new HashSet<string>();

            if (definition.AdditionalSources != null)
            {
                foreach (var ext in definition.AdditionalSources)
                {
                    var tree = SyntaxFactory.ParseSyntaxTree(AddUsingIndexStatic(ext.Value));
                    syntaxTrees.Add(tree);

                    var ns = tree.GetRoot().DescendantNodes()
                        .OfType<NamespaceDeclarationSyntax>()
                        .FirstOrDefault();

                    if (ns != null)
                    {
                        usings.Add(ns.Name.ToString());
                    }
                }
            }

            if (definition.AdditionalAssemblies != null)
            {
                foreach (var additionalAssembly in definition.AdditionalAssemblies)
                {
                    if (additionalAssembly.Usings != null)
                    {
                        foreach (var @using in additionalAssembly.Usings)
                            usings.Add(@using);
                    }
                }
            }

            if (usings.Count > 0)
            {
                //Adding using directive with duplicates to avoid O(n*m) operation and confusing code
                var newUsing = usings.Select(x => SyntaxFactory.UsingDirective(SyntaxFactory.ParseName(x))).ToList();
                newUsing.AddRange(Usings);
                result.UsingDirectiveSyntaxes = newUsing.ToArray();
            }
            else
            {
                result.UsingDirectiveSyntaxes = Usings;
            }

            result.References = GetReferences(definition);

            var tempCompilation = CSharpCompilation.Create(
                assemblyName: string.Empty,
                syntaxTrees: syntaxTrees,
                references: result.References,
                options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
                    .WithOptimizationLevel(EnableDebugging ? OptimizationLevel.Debug : OptimizationLevel.Release)
            );

            var rewriter = new MethodDynamicParametersRewriter();
            result.SyntaxTrees = new List<SyntaxTree>();

            foreach (var tree in syntaxTrees) //now do the rewrites
            {
                rewriter.SemanticModel = tempCompilation.GetSemanticModel(tree);

                var rewritten = rewriter.Visit(tree.GetRoot()).NormalizeWhitespace();
                result.SyntaxTrees.Add(SyntaxFactory.SyntaxTree(rewritten, new CSharpParseOptions(documentationMode: DocumentationMode.None)));
            }

            return result;
        }

        private static string AddUsingIndexStatic(string ext)
        {
            return $"using {IndexesStaticNamespace};{Environment.NewLine}{ext}";
        }

        private static MetadataReference[] GetReferences(IndexDefinition definition)
        {
            var additionalAssemblies = AdditionalAssemblies.Value;
            var newReferences = new List<MetadataReference>();
            for (var i = 0; i < References.Length; i++)
                newReferences.Add(References[i]);

            foreach (var additionalAssembly in AdditionalAssemblies.Value.Values)
            {
                if (additionalAssembly.AssemblyType != AdditionalAssemblyType.BaseDirectory)
                    continue;

                newReferences.Add(additionalAssembly.AssemblyMetadataReference);
            }

            if (definition.AdditionalAssemblies != null)
            {
                foreach (var additionalAssembly in definition.AdditionalAssemblies)
                {
                    if (additionalAssembly.AssemblyName != null)
                    {
                        newReferences.Add(FromAssemblyName(additionalAssembly.AssemblyName));
                        continue;
                    }

                    if (additionalAssembly.AssemblyPath != null)
                    {
                        newReferences.Add(FromAssemblyPath(additionalAssembly.AssemblyPath));
                        continue;
                    }

                    if (additionalAssembly.PackageName != null)
                    {
                        newReferences.AddRange(FromPackage(additionalAssembly.PackageName, additionalAssembly.PackageVersion, additionalAssembly.PackageSourceUrl));
                        continue;
                    }

                    if (additionalAssembly.Usings != null && additionalAssembly.Usings.Count > 0)
                        continue;

                    throw new NotSupportedException($"Not supported additional assembly: {additionalAssembly}");
                }
            }

            return newReferences.ToArray();

            static MetadataReference FromAssemblyName(string name)
            {
                try
                {
                    var assemblyName = new AssemblyName(name);
                    var assembly = Assembly.Load(assemblyName);
                    return RegisterAssembly(assembly);
                }
                catch (Exception e)
                {
                    throw new IndexCompilationException($"Cannot load assembly '{name}'.", e);
                }
            }

            static MetadataReference FromAssemblyPath(string path)
            {
                try
                {
                    var assembly = LoadAssembly(path);
                    return RegisterAssembly(assembly);
                }
                catch (Exception e)
                {
                    throw new IndexCompilationException($"Cannot load assembly from path '{path}'.", e);
                }
            }

            static List<MetadataReference> FromPackage(string packageName, string packageVersion, string packageSourceUrl)
            {
                try
                {
                    if (string.IsNullOrWhiteSpace(packageName))
                        throw new ArgumentException($"'{nameof(packageName)}' cannot be null or whitespace", nameof(packageName));

                    if (string.IsNullOrWhiteSpace(packageVersion))
                        throw new ArgumentException($"'{nameof(packageVersion)}' cannot be null or whitespace", nameof(packageVersion));

                    var paths = AsyncHelpers.RunSync(() => MultiSourceNuGetFetcher.Instance.DownloadAsync(packageName, packageVersion, packageSourceUrl));
                    if (paths == null)
                        throw new InvalidOperationException($"NuGet package '{packageName}' version '{packageVersion}' from '{packageSourceUrl ?? MultiSourceNuGetFetcher.Instance.DefaultPackageSourceUrl}' does not exist.");

                    var references = new List<MetadataReference>();

                    foreach (var path in paths)
                    {
                        var assembly = LoadAssembly(path);
                        references.Add(RegisterAssembly(assembly));
                    }

                    return references;
                }
                catch (Exception e)
                {
                    throw new IndexCompilationException($"Cannot load NuGet package '{packageName}' version '{packageVersion}' from '{packageSourceUrl ?? MultiSourceNuGetFetcher.Instance.DefaultPackageSourceUrl}'.", e);
                }
            }

            static MetadataReference RegisterAssembly(Assembly assembly)
            {
                return AdditionalAssemblies.Value.GetOrAdd(assembly.FullName, _ => new AdditionalAssemblyServerSide(assembly.GetName(), assembly, CreateMetadataReferenceFromAssembly(assembly), AdditionalAssemblyType.Package)).AssemblyMetadataReference;
            }

            static Assembly LoadAssembly(string path)
            {
                try
                {
                    // this allows us to load assembly from runtime if there is a newer one
                    // e.g. when we are using newer runtime
                    var assemblyName = AssemblyName.GetAssemblyName(path);
                    return Assembly.Load(assemblyName);
                }
                catch
                {
                }

                return Assembly.LoadFile(path);
            }
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
                statements.AddRange(HandleMap(definition.SourceType, map, fieldNamesValidator, methodDetector, ref members));
            }

            if (string.IsNullOrWhiteSpace(definition.Reduce) == false)
            {
                statements.Add(HandleReduce(definition.Reduce, fieldNamesValidator, methodDetector, out CompiledIndexField[] groupByFields));

                var groupByFieldsArray = GetArrayCreationExpression<CompiledIndexField>(
                    groupByFields,
                    (builder, field) => field.WriteTo(builder));

                statements.Add(RoslynHelper.This(nameof(AbstractStaticIndexBase.GroupByFields)).Assign(groupByFieldsArray).AsExpressionStatement());
            }

            var fields = GetIndexedFields(definition, fieldNamesValidator);

            var outputFieldsArray = GetArrayCreationExpression<string>(
                fields,
                (builder, field) => builder.Append("\"").Append(field.Name).Append("\""));

            statements.Add(RoslynHelper.This(nameof(AbstractStaticIndexBase.OutputFields)).Assign(outputFieldsArray).AsExpressionStatement());

            var methods = methodDetector.Methods;

            if (methods.HasCreateField)
                statements.Add(RoslynHelper.This(nameof(AbstractStaticIndexBase.HasDynamicFields)).Assign(SyntaxFactory.LiteralExpression(SyntaxKind.TrueLiteralExpression)).AsExpressionStatement());

            if (methods.HasBoost)
                statements.Add(RoslynHelper.This(nameof(AbstractStaticIndexBase.HasBoostedFields)).Assign(SyntaxFactory.LiteralExpression(SyntaxKind.TrueLiteralExpression)).AsExpressionStatement());

            var ctor = RoslynHelper.PublicCtor(name)
                .AddBodyStatements(statements.ToArray());

            var @class = RoslynHelper.PublicClass(name);

            switch (definition.SourceType)
            {
                case IndexSourceType.Documents:
                    @class = @class
                        .WithBaseClass<StaticIndexBase>();
                    break;

                case IndexSourceType.TimeSeries:
                    @class = @class
                        .WithBaseClass<StaticTimeSeriesIndexBase>();
                    break;

                case IndexSourceType.Counters:
                    @class = @class
                        .WithBaseClass<StaticCountersIndexBase>();
                    break;

                default:
                    throw new NotSupportedException($"Not supported source type '{definition.SourceType}'.");
            }

            return @class
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

        private static List<StatementSyntax> HandleMap(IndexSourceType type, string map, FieldNamesValidator fieldNamesValidator, MethodDetectorRewriter methodsDetector,
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
                {
                    switch (type)
                    {
                        case IndexSourceType.Documents:
                            return HandleSyntaxInMap(fieldNamesValidator, new MapFunctionProcessor(CollectionNameRetriever.QuerySyntax, SelectManyRewriter.QuerySyntax), queryExpression, ref members);

                        case IndexSourceType.TimeSeries:
                            return HandleSyntaxInTimeSeriesMap(fieldNamesValidator, new MapFunctionProcessor(TimeSeriesCollectionNameRetriever.QuerySyntax, SelectManyRewriter.QuerySyntax), queryExpression, ref members);

                        case IndexSourceType.Counters:
                            return HandleSyntaxInCountersMap(fieldNamesValidator, new MapFunctionProcessor(CountersCollectionNameRetriever.QuerySyntax, SelectManyRewriter.QuerySyntax), queryExpression, ref members);

                        default:
                            throw new NotSupportedException($"Not supported source type '{type}'.");
                    }
                }

                var invocationExpression = expression as InvocationExpressionSyntax;
                if (invocationExpression != null)
                {
                    switch (type)
                    {
                        case IndexSourceType.Documents:
                            return HandleSyntaxInMap(fieldNamesValidator, new MapFunctionProcessor(CollectionNameRetriever.MethodSyntax, SelectManyRewriter.MethodSyntax), invocationExpression, ref members);

                        case IndexSourceType.TimeSeries:
                            return HandleSyntaxInTimeSeriesMap(fieldNamesValidator, new MapFunctionProcessor(TimeSeriesCollectionNameRetriever.MethodSyntax, SelectManyRewriter.MethodSyntax), invocationExpression, ref members);

                        case IndexSourceType.Counters:
                            return HandleSyntaxInCountersMap(fieldNamesValidator, new MapFunctionProcessor(CountersCollectionNameRetriever.MethodSyntax, SelectManyRewriter.MethodSyntax), invocationExpression, ref members);

                        default:
                            throw new NotSupportedException($"Not supported source type '{type}'.");
                    }
                }

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
                        throw new InvalidOperationException($"Group by field '{groupByField.Name}' was not found on the list of index fields ({string.Join(", ", fieldNamesValidator.Fields.Select(x => x.Name))})");
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

        private static List<StatementSyntax> HandleSyntaxInCountersMap(FieldNamesValidator fieldValidator, MapFunctionProcessor mapRewriter, ExpressionSyntax expression,
            ref SyntaxList<MemberDeclarationSyntax> members)
        {
            var (results, mapExpression) = HandleSyntaxInMapBase(fieldValidator, mapRewriter, expression, ref members, identifier: "counters");

            var collectionRetriever = (CollectionNameRetrieverBase)mapRewriter.CollectionRetriever;

            var collections = collectionRetriever.Collections?.ToArray()
                ?? new CollectionNameRetrieverBase.Collection[] { new CollectionNameRetrieverBase.Collection(Constants.Documents.Collections.AllDocumentsCollection, Constants.Counters.All) };

            foreach (var c in collections)
            {
                var collectionName = c.CollectionName;
                if (string.IsNullOrWhiteSpace(collectionName))
                    throw new InvalidOperationException("Collection name cannot be null or whitespace.");

                var collection = SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, SyntaxFactory.Literal(collectionName));

                var counterName = c.ItemName;
                if (string.IsNullOrWhiteSpace(counterName))
                    counterName = Constants.Counters.All;

                var counter = SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, SyntaxFactory.Literal(counterName));

                results.Add(RoslynHelper.This(nameof(StaticCountersIndexBase.AddMap)).Invoke(collection, counter, mapExpression).AsExpressionStatement()); // this.AddMap("Users", "Likes", counters => from counter in counters ... )

                if (mapRewriter.ReferencedCollections != null)
                {
                    foreach (var referencedCollection in mapRewriter.ReferencedCollections)
                    {
                        var rc = SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, SyntaxFactory.Literal(referencedCollection));
                        results.Add(RoslynHelper.This(nameof(StaticCountersIndexBase.AddReferencedCollection)).Invoke(collection, rc).AsExpressionStatement());
                    }
                }

                if (mapRewriter.HasLoadCompareExchangeValue)
                    results.Add(RoslynHelper.This(nameof(StaticIndexBase.AddCompareExchangeReferenceToCollection)).Invoke(collection).AsExpressionStatement());
            }

            return results;
        }

        private static List<StatementSyntax> HandleSyntaxInTimeSeriesMap(FieldNamesValidator fieldValidator, MapFunctionProcessor mapRewriter, ExpressionSyntax expression,
            ref SyntaxList<MemberDeclarationSyntax> members)
        {
            var (results, mapExpression) = HandleSyntaxInMapBase(fieldValidator, mapRewriter, expression, ref members, identifier: "timeSeries");

            var collectionRetriever = (CollectionNameRetrieverBase)mapRewriter.CollectionRetriever;

            var collections = collectionRetriever.Collections?.ToArray()
                ?? new CollectionNameRetrieverBase.Collection[] { new CollectionNameRetrieverBase.Collection(Constants.Documents.Collections.AllDocumentsCollection, Constants.TimeSeries.All) };

            foreach (var c in collections)
            {
                var collectionName = c.CollectionName;
                if (string.IsNullOrWhiteSpace(collectionName))
                    throw new InvalidOperationException("Collection name cannot be null or whitespace.");

                var collection = SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, SyntaxFactory.Literal(collectionName));

                var timeSeriesName = c.ItemName;
                if (string.IsNullOrWhiteSpace(timeSeriesName))
                    timeSeriesName = Constants.TimeSeries.All;

                var timeSeries = SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, SyntaxFactory.Literal(timeSeriesName));

                results.Add(RoslynHelper.This(nameof(StaticTimeSeriesIndexBase.AddMap)).Invoke(collection, timeSeries, mapExpression).AsExpressionStatement()); // this.AddMap("Users", "HeartBeat", timeSeries => from ts in timeSeries ... )

                if (mapRewriter.ReferencedCollections != null)
                {
                    foreach (var referencedCollection in mapRewriter.ReferencedCollections)
                    {
                        var rc = SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, SyntaxFactory.Literal(referencedCollection));
                        results.Add(RoslynHelper.This(nameof(StaticTimeSeriesIndexBase.AddReferencedCollection)).Invoke(collection, rc).AsExpressionStatement());
                    }
                }

                if (mapRewriter.HasLoadCompareExchangeValue)
                    results.Add(RoslynHelper.This(nameof(StaticIndexBase.AddCompareExchangeReferenceToCollection)).Invoke(collection).AsExpressionStatement());
            }

            return results;
        }

        private static List<StatementSyntax> HandleSyntaxInMap(FieldNamesValidator fieldValidator, MapFunctionProcessor mapRewriter, ExpressionSyntax expression,
            ref SyntaxList<MemberDeclarationSyntax> members)
        {
            var (results, mapExpression) = HandleSyntaxInMapBase(fieldValidator, mapRewriter, expression, ref members, identifier: "docs");

            var collectionRetriever = (CollectionNameRetriever)mapRewriter.CollectionRetriever;
            var collectionNames = collectionRetriever.CollectionNames ?? new[] { Constants.Documents.Collections.AllDocumentsCollection };

            foreach (var cName in collectionNames)
            {
                var collectionName = string.IsNullOrWhiteSpace(cName)
                    ? Constants.Documents.Collections.AllDocumentsCollection
                    : cName;

                var collection = SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, SyntaxFactory.Literal(collectionName));

                results.Add(RoslynHelper.This(nameof(StaticIndexBase.AddMap)).Invoke(collection, mapExpression).AsExpressionStatement()); // this.AddMap("Users", docs => from doc in docs ... )

                if (mapRewriter.ReferencedCollections != null)
                {
                    foreach (var referencedCollection in mapRewriter.ReferencedCollections)
                    {
                        var rc = SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, SyntaxFactory.Literal(referencedCollection));
                        results.Add(RoslynHelper.This(nameof(StaticIndexBase.AddReferencedCollection)).Invoke(collection, rc).AsExpressionStatement());
                    }
                }

                if (mapRewriter.HasLoadCompareExchangeValue)
                    results.Add(RoslynHelper.This(nameof(StaticIndexBase.AddCompareExchangeReferenceToCollection)).Invoke(collection).AsExpressionStatement());
            }

            return results;
        }

        private static (List<StatementSyntax> Results, ExpressionSyntax MapExpression) HandleSyntaxInMapBase(FieldNamesValidator fieldValidator, MapFunctionProcessor mapRewriter, ExpressionSyntax expression,
            ref SyntaxList<MemberDeclarationSyntax> members, string identifier)
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

            var results = new List<StatementSyntax>();
            ExpressionSyntax mapExpression;

            if (optimized != null)
            {
                var method = SyntaxFactory.MethodDeclaration(SyntaxFactory.IdentifierName("IEnumerable"), SyntaxFactory.Identifier("Map_" + members.Count))
                    .WithParameterList(
                        SyntaxFactory.ParameterList(SyntaxFactory.SingletonSeparatedList(
                            SyntaxFactory.Parameter(SyntaxFactory.Identifier(identifier))
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

                mapExpression = RoslynHelper.This(method.Identifier.Text);
            }
            else
            {
                mapExpression = SyntaxFactory.SimpleLambdaExpression(SyntaxFactory.Parameter(SyntaxFactory.Identifier(identifier)), rewrittenExpression);
            }

            return (results, mapExpression);
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
            return $"{IndexNamePrefix}{Regex.Replace(name, @"[^\w\d]", "_")}";
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

        private class AdditionalAssemblyServerSide
        {
            public AdditionalAssemblyServerSide(AssemblyName name, Assembly assembly, MetadataReference reference, AdditionalAssemblyType type)
            {
                AssemblyName = name ?? throw new ArgumentNullException(nameof(name));
                Assembly = assembly ?? throw new ArgumentNullException(nameof(assembly));
                AssemblyMetadataReference = reference ?? throw new ArgumentNullException(nameof(reference));
                AssemblyType = type;
            }

            public AssemblyName AssemblyName { get; }
            public Assembly Assembly { get; }
            public MetadataReference AssemblyMetadataReference { get; }
            public AdditionalAssemblyType AssemblyType { get; }
        }

        public enum AdditionalAssemblyType
        {
            BaseDirectory,
            Package
        }
    }
}
