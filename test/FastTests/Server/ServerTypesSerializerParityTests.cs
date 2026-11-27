using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using FastTests.Client;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Commands.Indexes;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.Smuggler.Migration;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server;

public class ServerTypesSerializerParityTests(ITestOutputHelper output) : SerializerParityTestBase(output)
{
    private static DocumentConventions ServerConventions => DocumentConventions.DefaultForServer;

    public static TheoryData<Type> WrittenThroughConventions => new()
    {
        typeof(DatabaseRecord),
        typeof(AutoSpatialOptions),
        typeof(AutoVectorOptions),
        typeof(RestorePoints),
        typeof(ImportInfo),
        typeof(StorageIoAnalysisInfo),
        typeof(ConflictSolver),
        typeof(Raven.Server.Documents.Indexes.Test.TestIndexParameters),
        typeof(Raven.Server.Documents.Commands.ValidateUnusedIdsCommand.Parameters),
        typeof(Raven.Server.Documents.Revisions.RevertRevisionsRequest),
        typeof(Raven.Server.Smuggler.Migration.LastEtagsInfo),
        typeof(Raven.Client.Documents.Operations.TimeSeries.TimeSeriesRangeResult),
        typeof(Raven.Client.Documents.Operations.Revisions.DeleteRevisionsOperation.Parameters),
        typeof(Raven.Client.Documents.Operations.Revisions.AdoptOrphanedRevisionsOperation.Parameters),
        typeof(Raven.Client.Documents.Operations.Revisions.EnforceRevisionsConfigurationOperation.Parameters)
    };

    public static TheoryData<Type> ReadThroughConventions => new()
    {
        typeof(PagingPerformanceDetails),
        typeof(WarnIndexOutputsPerDocument),
        typeof(IndexingReferenceLoadWarning),
        typeof(SlowIoDetails),
        typeof(RequestLatencyDetail),
        typeof(ConflictPerformanceDetails),
        typeof(HugeDocumentsDetails),
        typeof(ServerLimitsDetails),
        typeof(ExternalReplication),
        typeof(PullReplicationAsHub),
        typeof(PullReplicationAsSink),
        typeof(InternalReplication),
        typeof(ConflictSolver),
        typeof(IncomingConnectionInfo),
        typeof(ConnectionShutdownInfo),
        typeof(GetIndexStalenessCommand.IndexStaleness),
        typeof(Raven.Server.SqlMigration.Model.MigrationRequest),
        typeof(Raven.Server.SqlMigration.Model.MigrationTestRequest),
        typeof(Raven.Server.Documents.TasksErrors.TaskErrors),
        typeof(Raven.Client.Documents.Indexes.IndexProgress),
        typeof(Raven.Client.Documents.Indexes.IndexesProgress),
        typeof(Raven.Server.Documents.Commands.Indexes.GetIndexesTotalTimeCommand.IndexesTotalTime),
        typeof(Raven.Server.Documents.Commands.Attachments.GetAttachmentHashCountCommand.Response),
        typeof(Raven.Server.Documents.Commands.Attachments.GetAttachmentMetadataWithCountsCommand.Response),
        typeof(Raven.Server.Documents.Commands.Queries.PatchByQueryTestCommand.Response),
        typeof(Raven.Client.Documents.Operations.OperationIdResult),
        typeof(Raven.Client.Documents.Queries.Facets.FacetOptions),
        typeof(Raven.Client.Documents.Queries.Suggestions.SuggestionOptions),
        typeof(Raven.Client.Documents.Queries.Highlighting.HighlightingOptions),
        typeof(Raven.Client.Documents.Queries.Explanation.ExplanationOptions),
        typeof(Raven.Client.Documents.Queries.Facets.FacetResult),
        typeof(Raven.Client.Documents.Queries.Suggestions.SuggestionResult),
        typeof(Raven.Server.Documents.Sharding.Queries.Suggestions.ShardedSuggestionResult.Popularity),
        typeof(Raven.Server.NotificationCenter.Notifications.Details.EtlTaskHealthChangeDetails),
        typeof(Raven.Server.NotificationCenter.Notifications.Details.EtlWarningDetails),
        typeof(Raven.Server.NotificationCenter.Notifications.Details.QueueSinkErrorsDetails),
        typeof(Raven.Server.NotificationCenter.Notifications.Details.SlowSqlDetails),
        typeof(Raven.Server.NotificationCenter.Notifications.Details.RemoteAttachmentsErrorsDetails)
    };

    [RavenTheory(RavenTestCategory.ClientApi)]
    [MemberData(nameof(WrittenThroughConventions))]
    public void SystemTextJsonWritesServerTypeLikeNewtonsoft(Type type)
    {
        object value = new SampleValues().Create(type);

        string expected = Write(value, Newtonsoft);
        AssertSameJson(expected, Write(value, ServerConventions));

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject json = Newtonsoft.Serialization.DefaultConverter.ToBlittable(value, context))
        {
            object readBack = ServerConventions.Serialization.DefaultConverter.FromBlittable(type, json);
            AssertSameJson(expected, Write(readBack, Newtonsoft));
        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void DatabaseRecordWrittenBySystemTextJsonIsReadByClusterDeserializer()
    {
        var record = (DatabaseRecord)new SampleValues().Create(typeof(DatabaseRecord));

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject fromNewtonsoft = Newtonsoft.Serialization.DefaultConverter.ToBlittable(record, context))
        using (BlittableJsonReaderObject fromSystemTextJson = ServerConventions.Serialization.DefaultConverter.ToBlittable(record, context))
        {
            DatabaseRecord expected = JsonDeserializationCluster.DatabaseRecord(fromNewtonsoft);
            DatabaseRecord actual = JsonDeserializationCluster.DatabaseRecord(fromSystemTextJson);

            AssertSameJson(Write(expected, Newtonsoft), Write(actual, Newtonsoft));
        }
    }

    [RavenTheory(RavenTestCategory.ClientApi)]
    [MemberData(nameof(ReadThroughConventions))]
    public void SystemTextJsonReadsServerTypeLikeNewtonsoft(Type type)
    {
        object value = new SampleValues().Create(type);

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject json = ToStoredJson(value, context))
        {
            object expected = Newtonsoft.Serialization.DefaultConverter.FromBlittable(type, json);
            object actual = ServerConventions.Serialization.DefaultConverter.FromBlittable(type, json);

            AssertSameJson(Write(expected, Newtonsoft), Write(actual, Newtonsoft));
        }
    }

    private static BlittableJsonReaderObject ToStoredJson(object value, JsonOperationContext context)
    {
        if (value is IDynamicJson dynamicJson)
            return context.ReadObject(dynamicJson.ToJson(), "stored");

        return Newtonsoft.Serialization.DefaultConverter.ToBlittable(value, context);
    }

    private static string Write(object value, Raven.Client.Documents.Conventions.DocumentConventions conventions)
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (BlittableJsonReaderObject json = conventions.Serialization.DefaultConverter.ToBlittable(value, context))
            return json.ToString();
    }

    private static void AssertSameJson(string expected, string actual)
    {
        string difference = FindDifference(JObject.Parse(expected), JObject.Parse(actual));
        Assert.True(difference == null, difference);
    }

    private static string FindDifference(JToken expected, JToken actual)
    {
        if (expected is JObject expectedObject && actual is JObject actualObject)
        {
            string[] expectedNames = expectedObject.Properties().Select(x => x.Name).ToArray();
            string[] actualNames = actualObject.Properties().Select(x => x.Name).ToArray();
            if (expectedNames.SequenceEqual(actualNames) == false)
                return $"{expected.Path}: expected members [{string.Join(", ", expectedNames)}] but got [{string.Join(", ", actualNames)}]";

            return expectedNames
                .Where(name => IsAssignedOnConstruction(expectedObject[name]) == false)
                .Select(name => FindDifference(expectedObject[name], actualObject[name]))
                .FirstOrDefault(x => x != null);
        }

        if (expected is JArray expectedArray && actual is JArray actualArray && expectedArray.Count == actualArray.Count)
            return expectedArray.Select((item, index) => FindDifference(item, actualArray[index])).FirstOrDefault(x => x != null);

        return JToken.DeepEquals(expected, actual) ? null : $"{expected.Path}: expected {expected.ToString(global::Newtonsoft.Json.Formatting.None)} but got {actual.ToString(global::Newtonsoft.Json.Formatting.None)}";
    }

    private static bool IsAssignedOnConstruction(JToken token)
    {
        return token.Path.Contains(nameof(Raven.Client.Documents.Operations.SchemaValidation.SchemaValidationConfiguration.ValidatorsPerCollection)) &&
               token.Path.EndsWith(nameof(Raven.Client.Documents.Operations.SchemaValidation.SchemaDefinition.LastModifiedTime));
    }

    private sealed class SampleValues
    {
        private const int MaxDepth = 8;

        private int _counter;

        public object Create(Type type)
        {
            return Create(type, depth: 0);
        }

        private object Create(Type type, int depth)
        {
            Type nullable = Nullable.GetUnderlyingType(type);
            if (nullable != null)
                return Create(nullable, depth);

            _counter++;

            if (type == typeof(string))
                return "value" + _counter;
            if (type == typeof(bool))
                return true;
            if (type == typeof(char))
                return 'c';
            if (type == typeof(Guid))
                return new Guid(_counter, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
            if (type == typeof(DateTime))
                return new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddMinutes(_counter);
            if (type == typeof(DateTimeOffset))
                return new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero).AddMinutes(_counter);
            if (type == typeof(TimeSpan))
                return TimeSpan.FromSeconds(_counter);
            if (type == typeof(double) || type == typeof(float) || type == typeof(decimal))
                return Convert.ChangeType(_counter + 0.5, type);
            if (type.IsPrimitive)
                return Convert.ChangeType(_counter % 100, type);
            if (type.IsEnum)
                return Enum.GetValues(type).Cast<object>().LastOrDefault() ?? Activator.CreateInstance(type);

            if (depth >= MaxDepth)
                return null;

            if (type.IsArray)
            {
                Array array = Array.CreateInstance(type.GetElementType(), 1);
                array.SetValue(Create(type.GetElementType(), depth + 1), 0);
                return array;
            }

            if (typeof(Delegate).IsAssignableFrom(type) || type == typeof(Type) || type == typeof(object) || type.IsPointer || type.IsByRef)
                return null;

            if (type.Namespace != null && type.Namespace.StartsWith("Sparrow", StringComparison.Ordinal))
                return null;

            if (type.IsInterface || type.IsAbstract)
                return CreateForAbstraction(type, depth);

            ConstructorInfo constructor = type.GetConstructors(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
                .OrderBy(x => x.GetParameters().Length)
                .FirstOrDefault();
            if (constructor == null && type.IsValueType == false)
                return null;

            object instance;
            try
            {
                instance = type.IsValueType
                    ? Activator.CreateInstance(type)
                    : constructor.Invoke(constructor.GetParameters().Select(x => Create(x.ParameterType, depth + 1)).ToArray());
            }
            catch (TargetInvocationException)
            {
                return null;
            }

            if (instance is IDictionary dictionary)
            {
                Fill(dictionary, type, depth);
                return instance;
            }

            if (instance is IEnumerable && TryFillCollection(instance, type, depth))
                return instance;

            foreach (FieldInfo field in type.GetFields(BindingFlags.Instance | BindingFlags.Public))
            {
                if (field.IsInitOnly || field.IsLiteral)
                    continue;

                field.SetValue(instance, Create(field.FieldType, depth + 1));
            }

            foreach (PropertyInfo property in type.GetProperties(BindingFlags.Instance | BindingFlags.Public))
            {
                if (property.GetIndexParameters().Length > 0)
                    continue;

                if (property.SetMethod == null || property.SetMethod.IsPublic == false)
                    continue;

                try
                {
                    property.SetValue(instance, Create(property.PropertyType, depth + 1));
                }
                catch (TargetInvocationException)
                {
                }
            }

            return instance;
        }

        private object CreateForAbstraction(Type type, int depth)
        {
            if (type.IsGenericType == false)
                return null;

            Type definition = type.GetGenericTypeDefinition();
            Type[] arguments = type.GetGenericArguments();

            if (definition == typeof(IDictionary<,>) || definition == typeof(IReadOnlyDictionary<,>))
                return Create(typeof(Dictionary<,>).MakeGenericType(arguments), depth);

            if (definition == typeof(IEnumerable<>) || definition == typeof(ICollection<>) || definition == typeof(IList<>) ||
                definition == typeof(IReadOnlyList<>) || definition == typeof(IReadOnlyCollection<>))
                return Create(typeof(List<>).MakeGenericType(arguments), depth);

            if (definition == typeof(ISet<>))
                return Create(typeof(HashSet<>).MakeGenericType(arguments), depth);

            return null;
        }

        private void Fill(IDictionary dictionary, Type type, int depth)
        {
            Type[] arguments = FindGenericArguments(type, typeof(IDictionary<,>));
            if (arguments == null)
                return;

            object key = Create(arguments[0], depth + 1);
            if (key == null)
                return;

            dictionary[key] = Create(arguments[1], depth + 1);
        }

        private bool TryFillCollection(object instance, Type type, int depth)
        {
            Type[] arguments = FindGenericArguments(type, typeof(IEnumerable<>));
            if (arguments == null)
                return false;

            MethodInfo add = type.GetMethod("Add", [arguments[0]]) ?? type.GetMethod("Enqueue", [arguments[0]]);
            if (add == null)
                return false;

            object item = Create(arguments[0], depth + 1);
            if (item != null)
                add.Invoke(instance, [item]);

            return true;
        }

        private static Type[] FindGenericArguments(Type type, Type genericInterface)
        {
            foreach (Type candidate in type.GetInterfaces().Append(type))
            {
                if (candidate.IsGenericType && candidate.GetGenericTypeDefinition() == genericInterface)
                    return candidate.GetGenericArguments();
            }

            return null;
        }
    }
}
