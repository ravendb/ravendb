using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Formatting;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal class IndexHandlerProcessorForConvertAutoIndex<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler>
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    where TOperationContext : JsonOperationContext
{
    public IndexHandlerProcessorForConvertAutoIndex([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    private string GetName() => RequestHandler.GetStringQueryString("name", required: true);

    private ConversionOutputType GetConvertType()
    {
        var typeAsString = RequestHandler.GetStringQueryString("outputType", required: true);

        if (Enum.TryParse(typeAsString, ignoreCase: true, out ConversionOutputType convertType) == false)
            throw new InvalidOperationException($"Could not parse '{typeAsString}' to any known conversion output type.");

        return convertType;
    }

    private bool HasDownload()
    {
        return RequestHandler.GetBoolValueQueryString("download", required: false) ?? false;
    }

    private void SetFileToDownload(string fileName)
    {
        HttpContext.Response.Headers[Constants.Headers.ContentDisposition] = $"attachment; filename=\"{fileName}\"; filename*=UTF-8''{fileName}";
    }

    public override async ValueTask ExecuteAsync()
    {
        var name = GetName();
        var type = GetConvertType();

        HttpContext.Response.Headers[Constants.Headers.ContentType] = "text/plain;charset=utf-8";

        using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            var record = RequestHandler.ServerStore.Cluster.ReadRawDatabaseRecord(context, RequestHandler.DatabaseName);
            if (record.AutoIndexes.TryGetValue(name, out var autoIndex) == false)
                throw IndexDoesNotExistException.ThrowForAuto(name);

            var sanitizedIndexName = AutoToStaticIndexConverter.GetSanitizedIndexName(autoIndex);

            switch (type)
            {
                case ConversionOutputType.CsharpClass:
                    var result = AutoToStaticIndexConverter.Instance.ConvertToAbstractIndexCreationTask(autoIndex);

                    if (HasDownload())
                        SetFileToDownload($"{sanitizedIndexName}.cs");

                    await using (var writer = new StreamWriter(RequestHandler.ResponseBodyStream()))
                        await writer.WriteLineAsync(result);
                    break;
                case ConversionOutputType.Json:
                    var definition = AutoToStaticIndexConverter.Instance.ConvertToIndexDefinition(autoIndex);

                    if (HasDownload())
                        SetFileToDownload($"{sanitizedIndexName}.json");

                    await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                    {
                        writer.WriteStartObject();

                        writer.WritePropertyName("Indexes");
                        writer.WriteStartArray();

                        writer.WriteIndexDefinition(context, definition);

                        writer.WriteEndArray();

                        writer.WriteEndObject();
                    }
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(type), $"Not supported conversion type '{type}'.");

            }
        }
    }
}

public enum ConversionOutputType
{
    CsharpClass,
    Json
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
            name = "Index/" + Regex.Replace(name, @"[^\w\d]", "/");

            while (true)
            {
                var newName = name.Replace("//", "/");
                if (newName == name)
                    break;

                name = newName;
            }

            return name;
        }

        static HashSet<string> ConstructMaps(AutoIndexDefinition autoIndex, AutoIndexConversionContext context)
        {
            var sb = new StringBuilder();
            sb.Append("from item in docs");

            if (autoIndex.Collection != Constants.Documents.Collections.AllDocumentsCollection && autoIndex.Collection != Constants.Documents.Collections.EmptyCollection)
                sb.Append($".{autoIndex.Collection}");

            sb
                .AppendLine()
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
                    var fieldNames = GenerateFieldName(kvp.Key, kvp.Value.Indexing);

                    foreach (var f in fieldNames)
                    {
                        HandleFieldIndexing(f.FieldName, f.Indexing);
                        HandleSpatial(f.FieldName, kvp.Value.Spatial, context);
                        HandleStorage(f.FieldName, kvp.Value.Storage);
                        HandleSuggestions(f.FieldName, kvp.Value.Suggestions);
                    }
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
                        throw new ArgumentOutOfRangeException(nameof(fieldStorage), $"Not supported field storage value '{fieldStorage.Value}'.");
                }
            }

            void HandleSuggestions(string fieldName, bool? suggestions)
            {
                if (suggestions.HasValue == false || suggestions == false)
                    return;

                var options = GetFieldOptions(fieldName);
                options.Suggestions = true;
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

        var csharpClassName = GetSanitizedIndexName(autoIndex);
        var className = autoIndex.Collection is Constants.Documents.Collections.AllDocumentsCollection or Constants.Documents.Collections.EmptyCollection
            ? "object"
            : Inflector.Singularize(autoIndex.Collection);

        sb
            .Append($"public class {csharpClassName} : {typeof(AbstractIndexCreationTask).FullName}<{className}");

        if (autoIndex.Type == IndexType.AutoMapReduce)
            sb.Append($", {csharpClassName}.Result");

        sb
            .AppendLine(">")
            .AppendLine("{")
            .AppendLine($"public {csharpClassName}()")
            .AppendLine("{");

        ConstructMap(autoIndex, sb, context);
        ConstructReduce(autoIndex, sb);
        ConstructFieldOptions(autoIndex, sb, context);

        sb
            .AppendLine("}");

        if (autoIndex.Type == IndexType.AutoMapReduce)
        {
            sb
                .AppendLine()
                .AppendLine("public class Result")
                .AppendLine("{");

            foreach (var kvp in autoIndex.MapFields)
            {
                var fieldNames = GenerateFieldName(kvp.Key, kvp.Value.Indexing);

                foreach (var f in fieldNames)
                {
                    switch (kvp.Value.Aggregation)
                    {
                        case AggregationOperation.Sum:
                        case AggregationOperation.Count:
                            {
                                sb.AppendLine($"public int {f.FieldName} {{ get; set; }}");
                                break;
                            }
                        default:
                            throw new ArgumentOutOfRangeException(nameof(kvp.Value.Aggregation), $"Not supported aggregation operation '{kvp.Value.Aggregation}'.");
                    }
                }
            }

            foreach (var kvp in autoIndex.GroupByFields)
            {
                var fieldNames = GenerateFieldName(kvp.Key, kvp.Value.Indexing);

                foreach (var f in fieldNames)
                {
                    if (f.IsConstant)
                        continue;

                    sb.AppendLine($"public object {f.FieldName} {{ get; set; }}");
                }
            }

            sb.AppendLine("}");
        }

        sb
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

            sb
                .AppendLine()
                .Append("Reduce = results => ");

            ConstructReduceInternal(sb, autoIndex);
        }

        static void ConstructFieldOptions(AutoIndexDefinition autoIndex, StringBuilder sb, AutoIndexConversionContext context)
        {
            sb.AppendLine();
            foreach (var kvp in autoIndex.MapFields)
            {
                var fieldNames = GenerateFieldName(kvp.Key, kvp.Value.Indexing);

                foreach (var f in fieldNames)
                {
                    HandleFieldIndexing(f.FieldName, f.Indexing);
                    HandleStorage(f.FieldName, kvp.Value.Storage);
                    HandleSuggestions(f.FieldName, kvp.Value.Suggestions);
                    HandleSpatial(f.FieldName, kvp.Value.Spatial, context);
                }
            }

            return;

            void HandleFieldIndexing(string fieldName, AutoFieldIndexing? indexing)
            {
                if (indexing.HasValue == false)
                    return;

                if (indexing.Value.HasFlag(AutoFieldIndexing.Search))
                {
                    sb.AppendLine($"Index(\"{fieldName}\", {typeof(FieldIndexing).FullName}.{nameof(FieldIndexing.Search)});");

                    if (indexing.Value.HasFlag(AutoFieldIndexing.Highlighting))
                        sb.AppendLine($"TermVector(\"{fieldName}\", {typeof(FieldTermVector).FullName}.{nameof(FieldTermVector.WithPositionsAndOffsets)});");
                }

                if (indexing.Value.HasFlag(AutoFieldIndexing.Exact))
                    sb.AppendLine($"Index(\"{fieldName}\", {typeof(FieldIndexing).FullName}.{nameof(FieldIndexing.Exact)});");
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

                sb.AppendLine($"Suggestion(x => x.{fieldName});");
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
                                throw new ArgumentOutOfRangeException(nameof(spatial.Strategy), $"Not supported spatial search strategy '{spatial.Strategy}'.");
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
                                throw new ArgumentOutOfRangeException(nameof(spatial.Strategy), $"Not supported spatial search strategy '{spatial.Strategy}'.");
                        }

                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(spatial.Type), $"Not supported spatial field type '{spatial.Type}'.");
                }

                sb.AppendLine(");");
            }
        }
    }

    public static string GetSanitizedIndexName(AutoIndexDefinition autoIndex)
    {
        var name = autoIndex.Name[AutoIndexNameFinder.AutoIndexPrefix.Length..];
        name = Regex.Replace(name, @"[^\w\d]", "_");
        return "Index_" + name;
    }

    private static void HandleMapFields(StringBuilder sb, AutoIndexDefinition autoIndex, AutoIndexConversionContext context)
    {
        var countOfFields = autoIndex.MapFields.Count + autoIndex.GroupByFields.Count;
        if (countOfFields == 0)
            throw new NotSupportedException("Cannot convert auto index with 0 fields");

        var spatialCounter = 0;
        foreach (var kvp in autoIndex.MapFields)
        {
            var fieldNames = GenerateFieldName(kvp.Key, kvp.Value.Indexing);

            foreach (var f in fieldNames)
            {
                if (f.FieldName.Contains("[]"))
                    throw new NotSupportedException($"Invalid field name '{f.FieldName}'.");

                if (kvp.Value.Spatial == null)
                {
                    if (f.FieldName.StartsWith("spatial_"))
                        throw new NotSupportedException($"Invalid field name '{f.FieldName}' with no spatial information.");

                    switch (kvp.Value.Aggregation)
                    {
                        case AggregationOperation.None:
                        case AggregationOperation.Sum:
                            {
                                var key = kvp.Key;
                                var fieldPath = key.StartsWith(Constants.Documents.Metadata.Key)
                                    ? $"MetadataFor(item)[\"{key[(Constants.Documents.Metadata.Key.Length + 1)..]}\"]"
                                    : $"item.{kvp.Key}";

                                sb.AppendLine($"{f.FieldName} = {fieldPath},");
                                break;
                            }
                        case AggregationOperation.Count:
                            sb.AppendLine($"{f.FieldName} = 1,");
                            break;
                        default:
                            throw new ArgumentOutOfRangeException(nameof(kvp.Value.Aggregation), $"Not supported aggregation operation '{kvp.Value.Aggregation}'.");
                    }
                }
                else
                {
                    var newFieldName = spatialCounter == 0 ? "Coordinates" : $"Coordinates{spatialCounter}";
                    context.FieldNameMapping.Add(f.FieldName, newFieldName);

                    switch (kvp.Value.Spatial.MethodType)
                    {
                        case AutoSpatialOptions.AutoSpatialMethodType.Point:
                        case AutoSpatialOptions.AutoSpatialMethodType.Wkt:
                            sb.AppendLine($"{newFieldName} = {nameof(AbstractIndexCreationTask.CreateSpatialField)}({string.Join(", ", kvp.Value.Spatial.MethodArguments.Select(x => $"item.{x}"))}),");
                            break;
                        default:
                            throw new ArgumentOutOfRangeException(nameof(kvp.Value.Spatial.MethodType), $"Not supported spatial method type '{kvp.Value.Spatial.MethodType}'.");
                    }

                    spatialCounter++;
                }
            }
        }

        if (autoIndex.Type == IndexType.AutoMapReduce)
        {
            foreach (var kvp in autoIndex.GroupByFields)
            {
                var fieldNames = GenerateFieldName(kvp.Key, kvp.Value.Indexing);

                foreach (var f in fieldNames)
                {
                    if (f.IsConstant)
                        continue;

                    if (f.FieldName.Contains("[]"))
                        throw new NotSupportedException($"Invalid field name '{f.FieldName}'.");

                    var fieldPath = $"item.{kvp.Key}";

                    sb.AppendLine($"{f.FieldName} = {fieldPath},");
                }
            }
        }
    }

    private static void ConstructReduceInternal(StringBuilder sb, AutoIndexDefinition autoIndex)
    {
        sb
            .AppendLine("from result in results")
            .AppendLine($"group result by {GenerateGroupBy(autoIndex)} into g")
            .AppendLine("select new")
            .AppendLine("{");

        foreach (var kvp in autoIndex.MapFields)
        {
            var fieldNames = GenerateFieldName(kvp.Key, kvp.Value.Indexing);

            foreach (var f in fieldNames)
            {
                switch (kvp.Value.Aggregation)
                {
                    case AggregationOperation.Count:
                        {
                            var fieldPath = $"g.Sum(x => x.{kvp.Key})";

                            sb.AppendLine($"{f.FieldName} = {fieldPath},");
                            break;
                        }
                    case AggregationOperation.Sum:
                        {
                            var fieldPath = $"g.Sum(x => x.{kvp.Key})";

                            sb.AppendLine($"{f.FieldName} = {fieldPath},");
                            break;
                        }
                    default:
                        throw new NotSupportedException($"Field '{kvp.Key}' has unsupported aggregation type '{kvp.Value.Aggregation}'.");
                }
            }
        }

        foreach (var kvp in autoIndex.GroupByFields)
        {
            var groupByField = GenerateFieldName(kvp.Key, AutoFieldIndexing.Default).Single();
            var fieldNames = GenerateFieldName(kvp.Key, kvp.Value.Indexing);

            foreach (var f in fieldNames)
            {
                if (f.IsConstant)
                    continue;

                var fieldPath = $"g.Key.{groupByField.FieldName}";

                sb.AppendLine($"{f.FieldName} = {fieldPath},");
            }
        }

        sb.AppendLine("};");

        return;

        static string GenerateGroupBy(AutoIndexDefinition autoIndex)
        {
            var seenConst = false;
            var sb = new StringBuilder();
            foreach (var fieldName in autoIndex.GroupByFieldNames)
            {
                if (sb.Length > 0)
                    sb.Append(", ");

                var f = GenerateFieldName(fieldName, indexing: null).Single();
                if (f.IsConstant)
                {
                    sb.Append(f.FieldName);
                    seenConst = true;
                }
                else
                {
                    if (seenConst)
                        throw new InvalidOperationException("Cannot generate complex group by because a constant field was encountered.");

                    sb
                        .Append("result.")
                        .Append(f.FieldName);
                }
            }

            if (seenConst)
                return sb.ToString();

            return $"new {{ {sb} }}";
        }
    }

    private static IEnumerable<(string FieldName, AutoFieldIndexing Indexing, bool IsConstant)> GenerateFieldName(string name, AutoFieldIndexing? indexing)
    {
        name = name
            .Replace("@", "")
            .Replace("-", "_")
            .Replace(".", "_");

        if (name == "null")
            throw new NotSupportedException("Field name 'null' is not supported.");

        if (int.TryParse(name, out _)) // const
        {
            yield return (name, AutoFieldIndexing.Default, true);
            yield break;
        }

        if (indexing.HasValue == false)
        {
            yield return (name, AutoFieldIndexing.Default, false);
            yield break;
        }

        if (indexing.Value == AutoFieldIndexing.No)
        {
            yield return (name, AutoFieldIndexing.No, false);
            yield break;
        }

        if (indexing.Value.HasFlag(AutoFieldIndexing.Default))
            yield return (name, AutoFieldIndexing.Default, false);

        if (indexing.Value.HasFlag(AutoFieldIndexing.Search))
        {
            if (indexing.Value.HasFlag(AutoFieldIndexing.Highlighting))
                yield return ($"{name}_Search", AutoFieldIndexing.Search | AutoFieldIndexing.Highlighting, false);
            else
                yield return ($"{name}_Search", AutoFieldIndexing.Search, false);
        }

        if (indexing.Value.HasFlag(AutoFieldIndexing.Exact))
            yield return ($"{name}_Exact", AutoFieldIndexing.Exact, false);
    }

    public class AutoIndexConversionContext
    {
        public Dictionary<string, string> FieldNameMapping = new Dictionary<string, string>();
    }
}
