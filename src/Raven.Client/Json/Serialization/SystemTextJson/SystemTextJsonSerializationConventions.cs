using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Json.Serialization.SystemTextJson.Internal;
using Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.SystemTextJson
{
    public class SystemTextJsonSerializationConventions : ISerializationConventions
    {
        private readonly BlittableJsonConverter _defaultConverter;
        private readonly ConcurrentDictionary<Type, Dictionary<string, JsonPropertyInfo>> _jsonProperties = new();
        private Action<JsonSerializerOptions> _customizeJsonSerializerOptions;
        private Func<Type, BlittableJsonReaderObject, object> _deserializeEntityFromBlittable;
        private bool _ignoreByRefMembers;
        private bool _ignoreUnsafeMembers;
        private JsonSerializerContext _sourceGenerationContext;
        private JsonSerializerOptions _options;

        public DocumentConventions Conventions { get; private set; }

        public SystemTextJsonSerializationConventions()
        {
            _defaultConverter = new BlittableJsonConverter(this);
            _ignoreByRefMembers = false;
            _ignoreUnsafeMembers = false;
            CustomizeJsonSerializerOptions = _ => { };
        }

        /// <summary>
        ///     Register an action to customize the JsonSerializerOptions used by the DocumentStore.
        /// </summary>
        public Action<JsonSerializerOptions> CustomizeJsonSerializerOptions
        {
            get => _customizeJsonSerializerOptions;
            set
            {
                Conventions?.AssertNotFrozen();
                _customizeJsonSerializerOptions = value;
            }
        }

        public Func<Type, BlittableJsonReaderObject, object> DeserializeEntityFromBlittable
        {
            get => _deserializeEntityFromBlittable;
            set
            {
                Conventions?.AssertNotFrozen();
                _deserializeEntityFromBlittable = value;
            }
        }

        public bool IgnoreByRefMembers
        {
            get => _ignoreByRefMembers;
            set
            {
                Conventions?.AssertNotFrozen();
                _ignoreByRefMembers = value;
            }
        }

        public bool IgnoreUnsafeMembers
        {
            get => _ignoreUnsafeMembers;
            set
            {
                Conventions?.AssertNotFrozen();
                _ignoreUnsafeMembers = value;
            }
        }

        /// <summary>
        ///     Set a source-generated <see cref="JsonSerializerContext"/>
        ///     to avoid runtime reflection. Types included in the context use compile-time
        ///     generated metadata; types not in the context fall back to reflection.
        /// </summary>
        public JsonSerializerContext SourceGenerationContext
        {
            get => _sourceGenerationContext;
            set
            {
                Conventions?.AssertNotFrozen();
                _sourceGenerationContext = value;
            }
        }

        internal JsonSerializerOptions Options
        {
            get
            {
                JsonSerializerOptions options = _options;
                if (options != null)
                    return options;

                options = CreateJsonSerializerOptions();
                _options = options;
                return options;
            }
        }

        void ISerializationConventions.Initialize(DocumentConventions conventions)
        {
            Conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));

            if (_deserializeEntityFromBlittable == null)
                _deserializeEntityFromBlittable = new SystemTextJsonBlittableEntitySerializer(this).EntityFromJsonStream;
        }

        IBlittableJsonConverter ISerializationConventions.DefaultConverter => _defaultConverter;

        public ISubscriptionsBlittableJsonConverter CreateConverter<T>(SubscriptionBatch<T> batch)
        {
            return new SubscriptionBlittableJsonConverter(this);
        }

        ISessionBlittableJsonConverter ISerializationConventions.CreateConverter(InMemoryDocumentSessionOperations session)
        {
            return new SessionBlittableJsonConverter(session);
        }

        IJsonSerializer ISerializationConventions.CreateDeserializer(CreateDeserializerOptions options)
        {
            return new SystemTextJsonJsonSerializer(Options);
        }

        IJsonSerializer ISerializationConventions.CreateSerializer(CreateSerializerOptions options)
        {
            return new SystemTextJsonJsonSerializer(Options);
        }

        IJsonWriter ISerializationConventions.CreateWriter(JsonOperationContext context)
        {
            return new SystemTextJsonBlittableWriter(context);
        }

        object ISerializationConventions.DeserializeEntityFromBlittable(Type type, BlittableJsonReaderObject json)
        {
            return DeserializeEntityFromBlittable(type, json);
        }

        T ISerializationConventions.DeserializeEntityFromBlittable<T>(BlittableJsonReaderObject json)
        {
            return (T)DeserializeEntityFromBlittable(typeof(T), json);
        }

        internal Dictionary<string, JsonPropertyInfo> GetJsonProperties(Type type)
        {
            return _jsonProperties.GetOrAdd(type, t =>
            {
                JsonSerializerOptions options = Options;
                StringComparer comparer = options.PropertyNameCaseInsensitive ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;
                var properties = new Dictionary<string, JsonPropertyInfo>(comparer);

                JsonTypeInfo typeInfo = options.GetTypeInfo(t);
                if (typeInfo.Kind != JsonTypeInfoKind.Object)
                    return properties;

                foreach (JsonPropertyInfo property in typeInfo.Properties)
                    properties[property.Name] = property;

                return properties;
            });
        }

        private JsonSerializerOptions CreateJsonSerializerOptions()
        {
            var ravenResolver = new RavenJsonTypeInfoResolver(this);

            var options = new JsonSerializerOptions
            {
                TypeInfoResolver = _sourceGenerationContext != null
                    ? ravenResolver.WithSourceGenerationContext(_sourceGenerationContext)
                    : ravenResolver,
                NumberHandling = JsonNumberHandling.AllowReadingFromString,
                PropertyNameCaseInsensitive = true,
                DefaultIgnoreCondition = JsonIgnoreCondition.Never
            };

            InitializeConverters(options);
            CustomizeJsonSerializerOptions(options);

            return options;
        }

        internal bool TryWriteEntityToStream(Stream stream, object entity, IMetadataDictionary metadata, IJsonSerializer serializer)
        {
            if (entity is IDynamicMetaObjectProvider)
                return false;

            var options = ((SystemTextJsonJsonSerializer)serializer).Options;
            Type type = entity.GetType();
            bool hasIdentityProperty = Conventions.GetIdentityProperty(type) != null;
            long start = stream.Position;

            RavenJsonTypeInfoResolver.RootEntity = hasIdentityProperty ? entity : null;
            RavenJsonTypeInfoResolver.RemovedIdentityProperty = false;
            try
            {
                JsonSerializer.Serialize(stream, entity, type, options);
            }
            finally
            {
                RavenJsonTypeInfoResolver.RootEntity = null;
            }

            if (hasIdentityProperty && RavenJsonTypeInfoResolver.RemovedIdentityProperty == false)
            {
                stream.SetLength(start);
                stream.Position = start;
                return false;
            }

            if (metadata != null && metadata.Count > 0)
            {
                long end = stream.Position;
                stream.Position = end - 1;
                if (stream.ReadByte() != '}')
                    throw new InvalidOperationException($"Entity of type '{type}' must serialize to a JSON object.");

                bool hasProperties = end - start > 2;
                stream.SetLength(end - 1);
                stream.Position = end - 1;
                if (hasProperties)
                    stream.WriteByte((byte)',');

                using (var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { SkipValidation = true }))
                {
                    writer.WritePropertyName(Constants.Documents.Metadata.Key);
                    WriteMetadata(writer, metadata);
                }

                stream.WriteByte((byte)'}');
            }

            return true;
        }

        internal static void WriteMetadata(Utf8JsonWriter writer, IMetadataDictionary metadata)
        {
            writer.WriteStartObject();
            foreach (var kvp in metadata)
            {
                writer.WritePropertyName(kvp.Key);
                WriteMetadataValue(writer, kvp.Value);
            }
            writer.WriteEndObject();
        }

        internal static void WriteMetadataValue(Utf8JsonWriter writer, object value)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();

            switch (value)
            {
                case string s:
                    writer.WriteStringValue(s);
                    break;
                case long l:
                    writer.WriteNumberValue(l);
                    break;
                case int i:
                    writer.WriteNumberValue(i);
                    break;
                case bool b:
                    writer.WriteBooleanValue(b);
                    break;
                case double d:
                    writer.WriteNumberValue(d);
                    break;
                case float f:
                    writer.WriteNumberValue(f);
                    break;
                case decimal dec:
                    writer.WriteNumberValue(dec);
                    break;
                case DateTime dt:
                    writer.WriteStringValue(dt.GetDefaultRavenFormat(isUtc: dt.Kind == DateTimeKind.Utc));
                    break;
                case DateTimeOffset dto:
                    writer.WriteStringValue(dto.UtcDateTime.GetDefaultRavenFormat(isUtc: true));
                    break;
                case TimeSpan ts:
                    writer.WriteStringValue(ts.ToString("c"));
                    break;
                case null:
                    writer.WriteNullValue();
                    break;
                case IMetadataDictionary dict:
                    WriteMetadata(writer, dict);
                    break;
                case object[] arr:
                    writer.WriteStartArray();
                    foreach (var item in arr)
                        WriteMetadataValue(writer, item);
                    writer.WriteEndArray();
                    break;
                default:
                    JsonSerializer.Serialize(writer, value);
                    break;
            }
        }

        private void InitializeConverters(JsonSerializerOptions options)
        {
            if (Conventions == null || Conventions.SaveEnumsAsIntegers == false)
                options.Converters.Add(new JsonStringEnumConverter());

            options.Converters.Add(DateTimeISO8601Converter.Instance);
            options.Converters.Add(DictionaryDateTimeKeysConverter.Instance);
            options.Converters.Add(ParametersConverter.Instance);
            options.Converters.Add(LinqEnumerableConverter.Instance);
            options.Converters.Add(MetadataDictionaryConverter.Instance);
            options.Converters.Add(SizeConverter.Instance);
#if FEATURE_DATEONLY_TIMEONLY_SUPPORT
            options.Converters.Add(DateOnlyConverter.Instance);
            options.Converters.Add(TimeOnlyConverter.Instance);
#endif
            options.Converters.Add(VectorConverter.Instance);
        }
    }
}
