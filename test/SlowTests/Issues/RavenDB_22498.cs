using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using FastTests;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Formatting;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.Indexes;
using Raven.Server.Json;
using Sparrow.Json;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22498 : RavenTestBase
{
    public RavenDB_22498(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task Can_Convert_Simple_Auto_Index()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.Query<Order>()
                    .Where(x => x.Employee == "HR" && x.ShipTo.City == "NY")
                    .Search(x => x.Company, "abc")
                    .ToListAsync();
            }

            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

            var autoIndex = record.AutoIndexes.Values.First();

            var result = AutoToStaticIndexConverter.Instance.ConvertToAbstractIndexCreationTask(autoIndex);
            var def = AutoToStaticIndexConverter.Instance.ConvertToIndexDefinition(autoIndex);

            await store.Maintenance.SendAsync(new PutIndexesOperation(def));
        }
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [InlineData("SlowTests.Data.RavenDB_22498.AutoIndexes.FastTests.json.gz")]
    public async Task Can_Convert_Auto_Indexes(string inputFile)
    {
        using (var store = GetDocumentStore())
        {
            await using (var fileStream = new GZipStream(typeof(RavenDB_22498).Assembly.GetManifestResourceStream(inputFile), CompressionMode.Decompress))
            using (var sr = new StreamReader(fileStream))
            {
                while (true)
                {
                    var s = await sr.ReadLineAsync();
                    if (string.IsNullOrWhiteSpace(s))
                        break;

                    var autoIndex = JsonConvert.DeserializeObject<AutoIndexDefinition>(s, new StringEnumConverter());

                    IndexDefinition def = null;
                    try
                    {
                        var result = AutoToStaticIndexConverter.Instance.ConvertToAbstractIndexCreationTask(autoIndex);
                        def = AutoToStaticIndexConverter.Instance.ConvertToIndexDefinition(autoIndex);
                    }
                    catch (NotSupportedException)
                    {
                        // ignore
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException($"Failed to convert index {Environment.NewLine}{s}", e);
                    }

                    if (def == null) 
                        continue;

                    await store.Maintenance.SendAsync(new PutIndexesOperation(def));
                    await store.Maintenance.SendAsync(new DeleteIndexOperation(def.Name));
                }
            }
        }
    }

    public class AutoToStaticIndexConverter
    {
        public static AutoToStaticIndexConverter Instance = new();

        private AutoToStaticIndexConverter()
        {

        }

        public IndexDefinition ConvertToIndexDefinition(AutoIndexDefinition autoIndex)
        {
            if (autoIndex == null)
                throw new ArgumentNullException(nameof(autoIndex));

            var context = new AutoIndexConversionContext();

            var indexDefinition = new IndexDefinition();
            indexDefinition.Name = GenerateName(autoIndex.Name);

            indexDefinition.Maps = ConstructMaps(autoIndex, context);
            indexDefinition.Reduce = ConstructReduce(autoIndex);
            indexDefinition.Fields = ConstructFields(autoIndex, context);

            return indexDefinition;

            static string GenerateName(string name)
            {
                name = name[AutoIndexNameFinder.AutoIndexPrefix.Length..];
                name = Regex.Replace(name, @"[^\w\d]", "/");

                while (true)
                {
                    var newName = name.Replace("//", "/");
                    if (newName == name)
                        break;

                    name = newName;
                }

                return "Index/" + name;
            }

            static HashSet<string> ConstructMaps(AutoIndexDefinition autoIndex, AutoIndexConversionContext context)
            {
                var sb = new StringBuilder();
                sb
                    .AppendLine($"from item in docs.{autoIndex.Collection}")
                    .AppendLine("select new")
                    .AppendLine("{");

                HandleMapFields(sb, autoIndex, context);

                sb.AppendLine("};");

                return [sb.ToString()];
            }

            static string ConstructReduce(AutoIndexDefinition autoIndex)
            {
                if (autoIndex.GroupByFields == null || autoIndex.GroupByFields.Count == 0)
                    return null;

                var sb = new StringBuilder();

                ConstructReduceInternal(sb, autoIndex);

                return sb.ToString();
            }

            static Dictionary<string, IndexFieldOptions> ConstructFields(AutoIndexDefinition autoIndex, AutoIndexConversionContext context)
            {
                Dictionary<string, IndexFieldOptions> fields = null;
                if (autoIndex.MapFields is { Count: > 0 })
                {
                    foreach (var kvp in autoIndex.MapFields)
                    {
                        var fieldName = GenerateFieldName(kvp.Key);

                        HandleFieldIndexing(fieldName, kvp.Value.Indexing);
                        HandleSpatial(fieldName, kvp.Value.Spatial, context);
                        HandleStorage(fieldName, kvp.Value.Storage);
                        HandleSuggestions(fieldName, kvp.Value.Suggestions);
                    }
                }

                return fields;

                IndexFieldOptions GetFieldOptions(string fieldName)
                {
                    fields ??= new Dictionary<string, IndexFieldOptions>();
                    if (fields.TryGetValue(fieldName, out var value) == false)
                        fields[fieldName] = value = new IndexFieldOptions();

                    return value;
                }

                void HandleFieldIndexing(string fieldName, AutoFieldIndexing? fieldIndexing)
                {
                    if (fieldIndexing.HasValue == false)
                        return;

                    if (fieldIndexing.Value.HasFlag(AutoFieldIndexing.Search))
                    {
                        var options = GetFieldOptions(fieldName);
                        options.Indexing = FieldIndexing.Search;
                        return;
                    }

                    if (fieldIndexing.Value.HasFlag(AutoFieldIndexing.Exact))
                    {
                        var options = GetFieldOptions(fieldName);
                        options.Indexing = FieldIndexing.Exact;
                        return;
                    }

                    if (fieldIndexing.Value.HasFlag(AutoFieldIndexing.Highlighting))
                    {
                        var options = GetFieldOptions(fieldName);
                        options.Indexing = FieldIndexing.Search;
                        options.Storage = FieldStorage.Yes;
                        options.TermVector = FieldTermVector.WithPositionsAndOffsets;
                        return;
                    }
                }

                void HandleStorage(string fieldName, FieldStorage? fieldStorage)
                {
                    if (fieldStorage.HasValue == false || fieldStorage == FieldStorage.No)
                        return;

                    var options = GetFieldOptions(fieldName);

                    switch (fieldStorage.Value)
                    {
                        case FieldStorage.Yes:
                            options.Storage = FieldStorage.Yes;
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }

                void HandleSuggestions(string fieldName, bool? suggestions)
                {
                    if (suggestions.HasValue == false || suggestions == false)
                        return;

                    throw new NotImplementedException();
                }

                void HandleSpatial(string fieldName, AutoSpatialOptions spatialOptions, AutoIndexConversionContext context)
                {
                    if (spatialOptions == null)
                        return;

                    var realFieldName = context.FieldNameMapping[fieldName];

                    var options = GetFieldOptions(realFieldName);
                    options.Spatial = spatialOptions;
                }
            }
        }

        public string ConvertToAbstractIndexCreationTask(AutoIndexDefinition autoIndex)
        {
            if (autoIndex == null)
                throw new ArgumentNullException(nameof(autoIndex));

            var context = new AutoIndexConversionContext();

            var sb = new StringBuilder();

            var name = GenerateClassName(autoIndex);
            var className = Inflector.Singularize(autoIndex.Collection);

            sb
                .AppendLine($"public class {name} : {typeof(AbstractIndexCreationTask).FullName}<{className}>") // TODO handle reduce
                .AppendLine("{")
                .AppendLine($"public {name}()")
                .AppendLine("{");

            ConstructMap(autoIndex, sb, context);
            ConstructReduce(autoIndex, sb);
            ConstructFieldOptions(autoIndex, sb, context);

            sb
            .AppendLine("}")
            .AppendLine("}");

            using (var workspace = new AdhocWorkspace())
            {
                var syntaxTree = SyntaxFactory
                    .ParseSyntaxTree(sb.ToString());

                var result = Formatter.Format(syntaxTree.GetRoot(), workspace);
                return result.ToString();
            }


            static void ConstructMap(AutoIndexDefinition autoIndex, StringBuilder sb, AutoIndexConversionContext context)
            {
                sb
                    .AppendLine("Map = items => from item in items")
                    .AppendLine("select new")
                    .AppendLine("{");

                HandleMapFields(sb, autoIndex, context);

                sb.AppendLine("};");
            }

            static void ConstructReduce(AutoIndexDefinition autoIndex, StringBuilder sb)
            {
                if (autoIndex.GroupByFields == null || autoIndex.GroupByFields.Count == 0)
                    return;

                sb.Append("Reduce = results => ");

                ConstructReduceInternal(sb, autoIndex);
            }

            static void ConstructFieldOptions(AutoIndexDefinition autoIndex, StringBuilder sb, AutoIndexConversionContext context)
            {
                sb.AppendLine();
                foreach (var kvp in autoIndex.MapFields)
                {
                    var fieldName = GenerateFieldName(kvp.Key);

                    HandleFieldIndexing(fieldName, kvp.Value.Indexing);
                    HandleStorage(fieldName, kvp.Value.Storage);
                    HandleSuggestions(fieldName, kvp.Value.Suggestions);
                    HandleSpatial(fieldName, kvp.Value.Spatial, context);
                }

                return;

                void HandleFieldIndexing(string fieldName, AutoFieldIndexing? indexing)
                {
                    if (indexing.HasValue == false)
                        return;

                    if (indexing.Value.HasFlag(AutoFieldIndexing.Search))
                        sb.AppendLine($"Index(\"{fieldName}\", {typeof(FieldIndexing).FullName}.{nameof(FieldIndexing.Search)});");
                }

                void HandleStorage(string fieldName, FieldStorage? fieldStorage)
                {
                    if (fieldStorage.HasValue == false || fieldStorage == FieldStorage.No)
                        return;

                    sb.AppendLine($"Store(\"{fieldName}\", {typeof(FieldStorage).FullName}.{nameof(FieldStorage.Yes)});");
                }

                void HandleSuggestions(string fieldName, bool? suggestions)
                {
                    if (suggestions.HasValue == false || suggestions == false)
                        return;

                    throw new NotImplementedException();
                }

                void HandleSpatial(string fieldName, AutoSpatialOptions spatial, AutoIndexConversionContext context)
                {
                    if (spatial == null)
                        return;

                    var realFieldName = context.FieldNameMapping[fieldName];

                    sb.Append($"Spatial(\"{realFieldName}\", factory => factory.{spatial.Type}.");

                    switch (spatial.Type)
                    {
                        case SpatialFieldType.Cartesian:

                            switch (spatial.Strategy)
                            {
                                case SpatialSearchStrategy.QuadPrefixTree:
                                    sb.Append($"{nameof(CartesianSpatialOptionsFactory.QuadPrefixTreeIndex)}({spatial.MaxTreeLevel}, new {nameof(SpatialBounds)} {{ {nameof(SpatialBounds.MaxX)} = {spatial.MaxX}, {nameof(SpatialBounds.MaxY)} = {spatial.MaxY}, {nameof(SpatialBounds.MinX)} = {spatial.MinX}, {nameof(SpatialBounds.MinY)} = {spatial.MinY} }})");
                                    break;
                                case SpatialSearchStrategy.BoundingBox:
                                    sb.Append($"{nameof(CartesianSpatialOptionsFactory.BoundingBoxIndex)}()");
                                    break;
                                default:
                                    throw new ArgumentOutOfRangeException();
                            }

                            break;
                        case SpatialFieldType.Geography:

                            switch (spatial.Strategy)
                            {
                                case SpatialSearchStrategy.QuadPrefixTree:
                                    sb.Append($"{nameof(GeographySpatialOptionsFactory.QuadPrefixTreeIndex)}({spatial.MaxTreeLevel}, {typeof(SpatialUnits).FullName}.{spatial.Units})");
                                    break;
                                case SpatialSearchStrategy.GeohashPrefixTree:
                                    sb.Append($"{nameof(GeographySpatialOptionsFactory.GeohashPrefixTreeIndex)}({spatial.MaxTreeLevel}, {typeof(SpatialUnits).FullName}.{spatial.Units})");
                                    break;
                                case SpatialSearchStrategy.BoundingBox:
                                    sb.Append($"{nameof(GeographySpatialOptionsFactory.BoundingBoxIndex)}({typeof(SpatialUnits).FullName}.{spatial.Units})");
                                    break;
                                default:
                                    throw new ArgumentOutOfRangeException();
                            }

                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }

                    sb.AppendLine(");");
                }
            }

            static string GenerateClassName(AutoIndexDefinition autoIndex)
            {
                var name = autoIndex.Name[AutoIndexNameFinder.AutoIndexPrefix.Length..];
                name = Regex.Replace(name, @"[^\w\d]", "_");
                return "Index_" + name;
            }
        }

        private static void HandleMapFields(StringBuilder sb, AutoIndexDefinition autoIndex, AutoIndexConversionContext context)
        {
            var spatialCounter = 0;
            foreach (var kvp in autoIndex.MapFields)
            {
                var fieldName = GenerateFieldName(kvp.Key);

                if (fieldName.Contains("[]"))
                    throw new NotSupportedException($"Invalid field name '{fieldName}'.");

                if (kvp.Value.Spatial == null)
                {
                    switch (kvp.Value.Aggregation)
                    {
                        case AggregationOperation.None:
                            {
                                var fieldPath = $"item.{kvp.Key}";

                                sb.AppendLine($"{fieldName} = {fieldPath},");
                                break;
                            }
                        case AggregationOperation.Count:
                        case AggregationOperation.Sum:
                            sb.AppendLine($"{fieldName} = 1,");
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
                else
                {
                    var newFieldName = spatialCounter == 0 ? "Coordinates" : $"Coordinates{spatialCounter}";
                    context.FieldNameMapping.Add(fieldName, newFieldName);

                    switch (kvp.Value.Spatial.MethodType)
                    {
                        case AutoSpatialOptions.AutoSpatialMethodType.Point:
                        case AutoSpatialOptions.AutoSpatialMethodType.Wkt:
                            sb.AppendLine($"{newFieldName} = {nameof(AbstractIndexCreationTask.CreateSpatialField)}({string.Join(", ", kvp.Value.Spatial.MethodArguments.Select(x => $"item.{x}"))}),");
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }

                    spatialCounter++;
                }
            }

            if (autoIndex.Type == IndexType.AutoMapReduce)
            {
                foreach (var kvp in autoIndex.GroupByFields)
                {
                    var fieldName = GenerateFieldName(kvp.Key);
                    var fieldPath = $"item.{kvp.Key}";

                    sb.AppendLine($"{fieldName} = {fieldPath},");
                }
            }
        }

        private static void ConstructReduceInternal(StringBuilder sb, AutoIndexDefinition autoIndex)
        {
            sb
                .AppendLine("from result in results")
                .AppendLine($"group result by new {{ {GenerateGroupBy(autoIndex)} }} into g")
                .AppendLine("select new")
                .AppendLine("{");

            foreach (var kvp in autoIndex.MapFields)
            {
                var fieldName = GenerateFieldName(kvp.Key);

                switch (kvp.Value.Aggregation)
                {
                    case AggregationOperation.Count:
                        {
                            var fieldPath = $"g.Count(x => x.{kvp.Key})";

                            sb.AppendLine($"{fieldName} = {fieldPath},");
                            break;
                        }
                    case AggregationOperation.Sum:
                        {
                            var fieldPath = $"g.Sum(x => x.{kvp.Key})";

                            sb.AppendLine($"{fieldName} = {fieldPath},");
                            break;
                        }
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }

            foreach (var kvp in autoIndex.GroupByFields)
            {
                var fieldName = GenerateFieldName(kvp.Key);
                var fieldPath = $"g.Key.{kvp.Key}";

                sb.AppendLine($"{fieldName} = {fieldPath},");
            }

            sb.AppendLine("};");

            return;

            static string GenerateGroupBy(AutoIndexDefinition autoIndex)
            {
                return string.Join(", ", autoIndex.GroupByFieldNames.Select(x => "result." + x));
            }
        }

        private static string GenerateFieldName(string name)
        {
            return name
                .Replace(".", "_");
        }

        public class AutoIndexConversionContext
        {
            public Dictionary<string, string> FieldNameMapping = new Dictionary<string, string>();
        }
    }
}


