using System;
using System.Collections.Generic;
using System.IO;
using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Server.Config;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Extensions;
using Raven.Server.ServerWide;
using Raven.Server.Web;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage;

public class DebugPackageEntries
{
    public class Entry
    {
        private static JsonSerializerOptions DeserializeOptions =
            new JsonSerializerOptions { IncludeFields = true, Converters =
            {
                new JsonStringEnumConverter(), 
                new OngoingTasksConverter()
            } };

        public string Name { get; set; }

        public Stream Content { get; set; }

        public JsonDocument Json { get; set; }

        public bool TryGetJsonValue<T>(string name, out T value)
        {
            if (Json.RootElement.TryGetProperty(name, out var element) == false)
            {
                value = default;
                return false;
            }

            value = element.Deserialize<T>(DeserializeOptions);
            return true;
        }

        public bool TryGetJson(string name, out JsonElement element)
        {
            if (Json.RootElement.TryGetProperty(name, out element) == false)
            {
                element = default;
                return false;
            }

            return true;
        }

        public T Deserialize<T>()
        {
            return Json.RootElement.Deserialize<T>(DeserializeOptions);
        }

        public async Task WriteContentToAsync(Stream stream)
        {
            // Entries are buffered into a MemoryStream when the package is read and kept for the
            // report's lifetime. The report is served on demand and re-read multiple times, so write
            // from the buffer instead of CopyToAsync - the latter advances the stream position and
            // would leave nothing for subsequent reads.
            if (Content is MemoryStream memoryStream && memoryStream.TryGetBuffer(out var buffer))
            {
                await stream.WriteAsync(buffer.AsMemory());
                return;
            }

            Content.Position = 0;
            await Content.CopyToAsync(stream);
        }
    }

    private Dictionary<string, Entry> _entries = new Dictionary<string, Entry>();

    public void Add(string entryName, Stream content, JsonDocument json)
    {
        _entries.Add(entryName, new Entry() { Name = entryName, Content = content, Json = json });
    }

    public bool TryGetValue<THandler, TValueType>(Expression<Func<THandler, object>> debugEndpoint, string fieldName, out TValueType value)
        where THandler : RequestHandler
    {
        if (TryGetEntry(debugEndpoint, out var entry) == false)
        {
            value = default;
            return false;
        }

        return entry.TryGetJsonValue(fieldName, out value);
    }

    public bool TryGetValue<THandler, TValueType>(Expression<Func<THandler, object>> debugEndpoint, out TValueType value) where THandler : RequestHandler
    {
        if (TryGetEntry(debugEndpoint, out var entry) == false)
        {
            value = default;
            return false;
        }

        value = entry.Deserialize<TValueType>();
        return true;
    }

    public bool TryGetEntry<T>(Expression<Func<T, object>> debugEndpoint, out Entry entry)
    {
        var entryName = DebugPackageExtensions.GetPackageEntryName(debugEndpoint);

        return _entries.TryGetValue(entryName, out entry);
    }

    public bool TryGetEntry(string path, string prefix, string extension, out Entry entry)
    {
        var entryName = DebugInfoPackageUtils.GetOutputPathFromRouteInformation(path, prefix, extension);

        return _entries.TryGetValue(entryName, out entry);
    }

    public bool TryGetValue<TValueType>(string path, out TValueType value)
    {
        if (TryGetEntry(path, string.Empty, "json", out var entry) == false)
        {
            value = default;
            return false;
        }

        value = entry.Deserialize<TValueType>();
        return true;
    }

    private class OngoingTasksConverter : JsonConverter<OngoingTask>
    {
        public override bool CanConvert(Type typeToConvert)
        {
            return typeToConvert == typeof(OngoingTask);
        }

        public override OngoingTask Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType != JsonTokenType.StartObject)
            {
                throw new JsonException();
            }

            using (var jsonDocument = JsonDocument.ParseValue(ref reader))
            {
                var jsonObject = jsonDocument.RootElement.GetRawText();
                if (jsonDocument.RootElement.TryGetProperty(nameof(OngoingTask.TaskType), out var taskType))
                {
                    if (Enum.TryParse<OngoingTaskType>(taskType.ToString(), out var type))
                    {
                        switch (type)
                        {
                            case OngoingTaskType.Backup:
                                return JsonSerializer.Deserialize<OngoingTaskBackup>(jsonObject, options);
                            case OngoingTaskType.Replication:
                                return JsonSerializer.Deserialize<OngoingTaskReplication>(jsonObject, options);
                            case OngoingTaskType.RavenEtl:
                                return JsonSerializer.Deserialize<OngoingTaskRavenEtl>(jsonObject, options);
                            case OngoingTaskType.SqlEtl:
                                return JsonSerializer.Deserialize<OngoingTaskSqlEtl>(jsonObject, options);
                            case OngoingTaskType.OlapEtl:
                                return JsonSerializer.Deserialize<OngoingTaskOlapEtl>(jsonObject, options);
                            case OngoingTaskType.ElasticSearchEtl:
                                return JsonSerializer.Deserialize<OngoingTaskElasticSearchEtl>(jsonObject, options);
                            case OngoingTaskType.QueueEtl:
                                return JsonSerializer.Deserialize<OngoingTaskQueueEtl>(jsonObject, options);
                            case OngoingTaskType.SnowflakeEtl:
                                return JsonSerializer.Deserialize<OngoingTaskSnowflakeEtl>(jsonObject, options);
                            case OngoingTaskType.Subscription:
                                return JsonSerializer.Deserialize<OngoingTaskSubscription>(jsonObject, options);
                            case OngoingTaskType.PullReplicationAsHub:
                                return JsonSerializer.Deserialize<OngoingTaskPullReplicationAsHub>(jsonObject, options);
                            case OngoingTaskType.PullReplicationAsSink:
                                return JsonSerializer.Deserialize<OngoingTaskPullReplicationAsSink>(jsonObject, options);
                            case OngoingTaskType.QueueSink:
                                return JsonSerializer.Deserialize<OngoingTaskQueueSink>(jsonObject, options);
                            case OngoingTaskType.CdcSink:
                                return JsonSerializer.Deserialize<OngoingTaskCdcSink>(jsonObject, options);
                            case OngoingTaskType.EmbeddingsGeneration:
                                return JsonSerializer.Deserialize<EmbeddingsGeneration>(jsonObject, options);
                            case OngoingTaskType.GenAi:
                                return JsonSerializer.Deserialize<GenAi>(jsonObject, options);
                            default:
                                throw new JsonException($"Unknown task type: {type}");
                        }
                    }
                }

                throw new JsonException("Could not determine task type");
            }
        }

        public override void Write(Utf8JsonWriter writer, OngoingTask value, JsonSerializerOptions options)
        {
            throw new NotImplementedException();
        }
    }

    private class ConfigurationEntryValueConverter : JsonConverter<ConfigurationEntryValue>
    {
        public override bool CanConvert(Type typeToConvert)
        {
            return typeToConvert == typeof(ConfigurationEntryValue);
        }
        public override ConfigurationEntryValue Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            throw new NotImplementedException();
        }

        public override void Write(Utf8JsonWriter writer, ConfigurationEntryValue value, JsonSerializerOptions options)
        {
            throw new NotImplementedException();
        }
    }
}
