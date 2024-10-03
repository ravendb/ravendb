﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
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
using Raven.Server.Logging;
using Sparrow.Logging;

namespace Raven.Server.Documents.Indexes.Static
{
    [SuppressMessage("ReSharper", "ConditionIsAlwaysTrueOrFalse")]
    public static class IndexCompiler
    {
        private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForServer(typeof(IndexCompiler));

        internal static readonly bool EnableDebugging = false; // for debugging purposes (mind https://issues.hibernatingrhinos.com/issue/RavenDB-6960)

        private const string IndexNamespace = "Raven.Server.Documents.Indexes.Static.Generated";

        internal const string IndexExtension = ".index";

        internal const string AdditionalSourceExtension = ".source";

        internal const string IndexNamePrefix = "Index_";

        private static readonly Lazy<ConcurrentDictionary<string, AdditionalAssemblyServerSide>> AdditionalAssemblies = new Lazy<ConcurrentDictionary<string, AdditionalAssemblyServerSide>>(DiscoverAdditionalAssemblies);

        private static readonly Assembly LuceneAssembly = typeof(Lucene.Net.Documents.Document).Assembly;

        private static readonly AssemblyName LuceneAssemblyName = LuceneAssembly.GetName();

        [ThreadStatic]
        private static bool DisableMatchingAdditionalAssembliesByNameValue;

        static IndexCompiler()
        {
            AssemblyLoadContext.Default.Resolving += (ctx, name) =>
            {
                if (AdditionalAssemblies.IsValueCreated)
                {
                    if (AdditionalAssemblies.Value.TryGetValue(name.FullName, out var assembly))
                        return assembly.Assembly;

                    if (DisableMatchingAdditionalAssembliesByNameValue == false && AdditionalAssemblies.Value.TryGetValue(name.Name, out assembly))
                        return assembly.Assembly;
                }

                if (name.Name == LuceneAssemblyName.Name)
                    return LuceneAssembly;

                return null;
            };
        }

        private static IDisposable DisableMatchingAdditionalAssembliesByName()
        {
            DisableMatchingAdditionalAssembliesByNameValue = true;

            return new DisposableAction(() =>
            {
                DisableMatchingAdditionalAssembliesByNameValue = false;
            });
        }

        private static ConcurrentDictionary<string, AdditionalAssemblyServerSide> DiscoverAdditionalAssemblies()
        {
            var results = new ConcurrentDictionary<string, AdditionalAssemblyServerSide>(StringComparer.OrdinalIgnoreCase);

            foreach (var path in Directory.GetFiles(AppContext.BaseDirectory, "*.dll"))
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Attempting to load additional assembly from '{path}'.");

                try
                {
                    var name = AssemblyLoadContext.GetAssemblyName(path);
                    var assembly = AssemblyLoadContext.Default.LoadFromAssemblyPath(path);
                    var reference = CreateMetadataReferenceFromAssembly(assembly);

                    var result = new AdditionalAssemblyServerSide(name, assembly, reference, AdditionalAssemblyType.BaseDirectory);
                    results.TryAdd(name.FullName, result);
                    results.TryAdd(name.Name, result);

                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Loaded additional assembly from '{path}' and registered it under '{name.Name}'.");
                }
                catch (Exception e)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Could not load additional assembly from '{path}'.", e);
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

        public static readonly MetadataReference[] References =
        {
            CreateMetadataReferenceFromAssembly(typeof(object).Assembly),
            CreateMetadataReferenceFromAssembly(typeof(ExpressionType).Assembly),
            CreateMetadataReferenceFromAssembly(typeof(Enumerable).Assembly),
            CreateMetadataReferenceFromAssembly(typeof(IndexCompiler).Assembly),
            CreateMetadataReferenceFromAssembly(typeof(BoostedValue).Assembly),
            CreateMetadataReferenceFromAssembly(LuceneAssembly),
            CreateMetadataReferenceFromAssembly(Assembly.Load(new AssemblyName("System.Runtime"))),
            CreateMetadataReferenceFromAssembly(Assembly.Load(new AssemblyName("Microsoft.CSharp"))),
            CreateMetadataReferenceFromAssembly(Assembly.Load(new AssemblyName("mscorlib"))),
            CreateMetadataReferenceFromAssembly(Assembly.Load(new AssemblyName("netstandard"))),
            CreateMetadataReferenceFromAssembly(Assembly.Load(new AssemblyName("System.Collections"))),
            CreateMetadataReferenceFromAssembly(typeof(Regex).Assembly),
            CreateMetadataReferenceFromAssembly(typeof(Uri).Assembly),
            CreateMetadataReferenceFromAssembly(typeof(IPAddress).Assembly)
        };

        private static unsafe MetadataReference CreateMetadataReferenceFromAssembly(Assembly assembly)
        {
            assembly.TryGetRawMetadata(out var blob, out var length);
            var moduleMetadata = ModuleMetadata.CreateFromMetadata((IntPtr)blob, length);
            var assemblyMetadata = AssemblyMetadata.Create(moduleMetadata);
            return assemblyMetadata.GetReference();
        }

        public static AbstractStaticIndexBase Compile(IndexDefinition definition, long indexVersion)
        {
            
            var cSharpSafeName = GetCSharpSafeName(definition.Name);

            var @class = CreateClass(cSharpSafeName, definition, indexVersion);

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

            var res = GetUsingDirectiveAndSyntaxTreesAndReferences(definition, cSharpSafeName);

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

        private static (UsingDirectiveSyntax[] UsingDirectiveSyntaxes, List<SyntaxTree> SyntaxTrees, MetadataReference[] References) GetUsingDirectiveAndSyntaxTreesAndReferences(IndexDefinition definition, string cSharpSafeName)
        {
            if (definition.AdditionalSources == null && definition.AdditionalAssemblies == null)
            {
                return (Usings, new List<SyntaxTree>(), References);
            }

            (UsingDirectiveSyntax[] UsingDirectiveSyntaxes, List<SyntaxTree> SyntaxTrees, MetadataReference[] References) result;
            var syntaxTrees = new Dictionary<string, SyntaxTree>();
            var usings = new HashSet<string>();

            if (definition.AdditionalSources != null)
            {
                foreach (var ext in definition.AdditionalSources)
                {
                    var tree = SyntaxFactory.ParseSyntaxTree(AddUsingIndexStatic(ext.Value));
                    syntaxTrees.Add(ext.Key, tree);

                    var ns = tree.GetRoot().DescendantNodes()
                        .OfType<BaseNamespaceDeclarationSyntax>()
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
                syntaxTrees: syntaxTrees.Values,
                references: result.References,
                options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
                    .WithOptimizationLevel(EnableDebugging ? OptimizationLevel.Debug : OptimizationLevel.Release)
            );

            var rewriter = new MethodDynamicParametersRewriter();
            result.SyntaxTrees = new List<SyntaxTree>();

            foreach (var kvp in syntaxTrees) //now do the rewrites
            {
                var sourceName = kvp.Key;
                var tree = kvp.Value;

                rewriter.SemanticModel = tempCompilation.GetSemanticModel(tree);

                var rewritten = rewriter.Visit(tree.GetRoot()).NormalizeWhitespace();

                SyntaxTree syntaxTree;

                if (EnableDebugging)
                {
                    var name = cSharpSafeName + "." + Guid.NewGuid() + "." + sourceName + AdditionalSourceExtension;

                    var sourceFile = Path.Combine(Path.GetTempPath(), name + ".cs");
                    File.WriteAllText(sourceFile, rewritten.ToFullString(), Encoding.UTF8);

                    syntaxTree = SyntaxFactory.ParseSyntaxTree(File.ReadAllText(sourceFile), path: sourceFile, encoding: Encoding.UTF8);
                }
                else
                {
                    syntaxTree = SyntaxFactory.SyntaxTree(rewritten, new CSharpParseOptions(documentationMode: DocumentationMode.None));
                }

                result.SyntaxTrees.Add(syntaxTree);
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

            static HashSet<MetadataReference> FromPackage(string packageName, string packageVersion, string packageSourceUrl)
            {
                try
                {
                    if (string.IsNullOrWhiteSpace(packageName))
                        throw new ArgumentException($"'{nameof(packageName)}' cannot be null or whitespace", nameof(packageName));

                    if (string.IsNullOrWhiteSpace(packageVersion))
                        throw new ArgumentException($"'{nameof(packageVersion)}' cannot be null or whitespace", nameof(packageVersion));

                    var package = AsyncHelpers.RunSync(() => MultiSourceNuGetFetcher.ForIndexes.DownloadAsync(packageName, packageVersion, packageSourceUrl));
                    if (package == null)
                        throw new InvalidOperationException($"NuGet package '{packageName}' version '{packageVersion}' from '{packageSourceUrl ?? MultiSourceNuGetFetcher.ForIndexes.DefaultPackageSourceUrl}' does not exist.");

                    var references = new HashSet<MetadataReference>();

                    RegisterPackage(package, userDefined: true, references);

                    NuGetNativeLibraryResolver.EnsureAssembliesRegisteredToNativeLibraries();

                    return references;
                }
                catch (Exception e)
                {
                    throw new IndexCompilationException($"Cannot load NuGet package '{packageName}' version '{packageVersion}' from '{packageSourceUrl ?? MultiSourceNuGetFetcher.ForIndexes.DefaultPackageSourceUrl}'.", e);
                }
            }

            static MetadataReference RegisterAssembly(Assembly assembly)
            {
                var assemblyName = assembly.GetName();

                if (AdditionalAssemblies.Value.TryGetValue(assemblyName.FullName, out var additionalAssemblyByFullName))
                    return additionalAssemblyByFullName.AssemblyMetadataReference;

                AdditionalAssemblies.Value.TryGetValue(assemblyName.Name, out var additionalAssemblyByName);

                var additionalAssembly = new AdditionalAssemblyServerSide(assemblyName, assembly, CreateMetadataReferenceFromAssembly(assembly), AdditionalAssemblyType.Package);

                AdditionalAssemblies.Value.TryAdd(assemblyName.FullName, additionalAssembly);

                if (additionalAssemblyByName == null || additionalAssemblyByName.AssemblyName.Version < assemblyName.Version)
                    AdditionalAssemblies.Value.TryAdd(assemblyName.Name, additionalAssembly);

                return additionalAssembly.AssemblyMetadataReference;
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

            static void RegisterPackage(NuGetFetcher.NuGetPackage package, bool userDefined, HashSet<MetadataReference> references)
            {
                if (package == null)
                    return;

                using (DisableMatchingAdditionalAssembliesByName())
                {
                    foreach (string library in package.Libraries)
                    {
                        var assembly = LoadAssembly(library);

                        if (userDefined)
                            NuGetNativeLibraryResolver.RegisterAssembly(assembly);

                        references.Add(RegisterAssembly(assembly));
                    }
                }

                if (userDefined)
                    NuGetNativeLibraryResolver.RegisterPath(package.NativePath);

                foreach (NuGetFetcher.NuGetPackage dependency in package.Dependencies)
                    RegisterPackage(dependency, userDefined: false, references);
            }
        }
        
        private static MemberDeclarationSyntax CreateClass(string name, IndexDefinition definition, long indexVersion)
        {
            var statements = new List<StatementSyntax>();
            var maps = definition.Maps.ToList();
            var fieldNamesValidator = new FieldNamesValidator();
            var methodDetector = new MethodDetectorRewriter(indexVersion);
            var stackDepthRetriever = new StackDepthRetriever();
            var members = new SyntaxList<MemberDeclarationSyntax>();
            var maxDepthInRecursiveLinqQuery = 0;
            
            for (var i = 0; i < maps.Count; i++)
            {
                var map = maps[i];
                statements.AddRange(HandleMap(definition.SourceType, map, fieldNamesValidator, methodDetector, stackDepthRetriever, ref members));
                
                maxDepthInRecursiveLinqQuery = Math.Max(maxDepthInRecursiveLinqQuery, stackDepthRetriever.StackSize);
                stackDepthRetriever.Clear();
            }

            if (string.IsNullOrWhiteSpace(definition.Reduce) == false)
            {
                statements.Add(HandleReduce(definition.Reduce, fieldNamesValidator, methodDetector, stackDepthRetriever, out CompiledIndexField[] groupByFields));

                var groupByFieldsArray = GetArrayCreationExpression<CompiledIndexField>(
                    groupByFields,
                    (builder, field) => field.WriteTo(builder));

                statements.Add(RoslynHelper.This(nameof(AbstractStaticIndexBase.GroupByFields)).Assign(groupByFieldsArray).AsExpressionStatement());
                
                maxDepthInRecursiveLinqQuery = Math.Max(maxDepthInRecursiveLinqQuery, stackDepthRetriever.StackSize);
            }

            var fields = GetIndexedFields(definition, fieldNamesValidator);

            var outputFieldsArray = GetArrayCreationExpression<string>(
                fields,
                (builder, field) => builder.Append("\"").Append(field.Name).Append("\""));

            statements.Add(RoslynHelper.This(nameof(AbstractStaticIndexBase.OutputFields)).Assign(outputFieldsArray).AsExpressionStatement());

            var methods = methodDetector.Methods;

            statements.Add(RoslynHelper.This(nameof(AbstractStaticIndexBase.StackSizeInSelectClause)).Assign(SyntaxFactory.ParseExpression($"{maxDepthInRecursiveLinqQuery}")).AsExpressionStatement());
            
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

        private static List<StatementSyntax> HandleMap(IndexSourceType type, string map, FieldNamesValidator fieldNamesValidator, MethodDetectorRewriter methodsDetector, StackDepthRetriever stackDepthRetriever,
            ref SyntaxList<MemberDeclarationSyntax> members)
        {
            try
            {
                map = NormalizeFunction(map);
                var expression = SyntaxFactory.ParseExpression(map).NormalizeWhitespace();

                fieldNamesValidator.Validate(map, expression);
                methodsDetector.Visit(expression);
                
                stackDepthRetriever.Visit(expression);
                stackDepthRetriever.VisitMethodQuery(map);
                
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

        private static StatementSyntax HandleReduce(string reduce, FieldNamesValidator fieldNamesValidator, MethodDetectorRewriter methodsDetector, StackDepthRetriever stackDepthRetriever, out CompiledIndexField[] groupByFields)
        {
            try
            {
                reduce = NormalizeFunction(reduce);
                var expression = SyntaxFactory.ParseExpression(reduce).NormalizeWhitespace();
                fieldNamesValidator?.Validate(reduce, expression);
                methodsDetector.Visit(expression);
                
                stackDepthRetriever.Visit(expression);
                stackDepthRetriever.VisitMethodQuery(reduce);
                
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
                //This is unoptimized case. We will scan C# code is manner thisMAX.[...]...doc;
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

        private sealed class CompilationResult
        {
            public Type Type { get; set; }
            public string Code { get; set; }
        }

        public sealed class IndexMethods
        {
            public bool HasLoadDocument { get; set; }

            public bool HasTransformWith { get; set; }

            public bool HasGroupBy { get; set; }

            public bool HasInclude { get; set; }

            public bool HasCreateField { get; set; }

            public bool HasBoost { get; set; }
        }

        private sealed class AdditionalAssemblyServerSide
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
