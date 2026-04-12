using System;
using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization;
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
        private BlittableJsonConverter _defaultConverter;
        private Action<JsonSerializerOptions> _customizeJsonSerializerOptions;
        private Func<Type, BlittableJsonReaderObject, object> _deserializeEntityFromBlittable;
        private bool _ignoreByRefMembers;
        private bool _ignoreUnsafeMembers;
        private JsonSerializerOptions _cachedSerializerOptions;
        private JsonSerializerOptions _cachedDeserializerOptions;

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
            JsonSerializerOptions jsonOptions = GetOrCreateOptions(ref _cachedDeserializerOptions);
            return new SystemTextJsonJsonSerializer(jsonOptions);
        }

        IJsonSerializer ISerializationConventions.CreateSerializer(CreateSerializerOptions options)
        {
            JsonSerializerOptions jsonOptions = GetOrCreateOptions(ref _cachedSerializerOptions);
            return new SystemTextJsonJsonSerializer(jsonOptions);
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

        private JsonSerializerOptions GetOrCreateOptions(ref JsonSerializerOptions cached)
        {
            if (cached != null)
                return cached;

            JsonSerializerOptions options = CreateJsonSerializerOptions();
            cached = options;
            return options;
        }

        internal JsonSerializerOptions CreateJsonSerializerOptions()
        {
            var options = new JsonSerializerOptions
            {
                TypeInfoResolver = new RavenJsonTypeInfoResolver(this),
                NumberHandling = JsonNumberHandling.AllowReadingFromString,
                PropertyNameCaseInsensitive = true,
                DefaultIgnoreCondition = JsonIgnoreCondition.Never
            };

            InitializeConverters(options);
            CustomizeJsonSerializerOptions(options);

            return options;
        }

        /// <summary>
        /// Writes an entity with @metadata directly to a stream as UTF-8 JSON,
        /// bypassing the blittable intermediate format. Used by bulk insert.
        /// </summary>
        internal void WriteEntityToStream(Stream stream, object entity, IMetadataDictionary metadata, IJsonSerializer serializer)
        {
            var options = ((SystemTextJsonJsonSerializer)serializer).Options;

            using var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { SkipValidation = true });
            writer.WriteStartObject();

            // Write @metadata first
            if (metadata != null && metadata.Count > 0)
            {
                writer.WritePropertyName(Constants.Documents.Metadata.Key);
                writer.WriteStartObject();
                foreach (var kvp in metadata)
                {
                    writer.WritePropertyName(kvp.Key);
                    WriteMetadataValue(writer, kvp.Value);
                }
                writer.WriteEndObject();
            }

            // Serialize entity to a JsonDocument so we can iterate its top-level properties
            // and write them into our existing object (which already has @metadata).
            using var doc = JsonDocument.Parse(JsonSerializer.SerializeToUtf8Bytes(entity, entity.GetType(), options));
            foreach (var property in doc.RootElement.EnumerateObject())
            {
                if (property.Name == Constants.Documents.Metadata.Key)
                    continue; // already written above
                property.WriteTo(writer);
            }

            writer.WriteEndObject();
            writer.Flush();
        }

        internal static void WriteMetadataValue(Utf8JsonWriter writer, object value)
        {
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
                    writer.WriteStartObject();
                    foreach (var kvp in dict)
                    {
                        writer.WritePropertyName(kvp.Key);
                        WriteMetadataValue(writer, kvp.Value);
                    }
                    writer.WriteEndObject();
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
            options.Converters.Add(LuceneDateTimeConverter.Instance);
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
            options.Converters.Add(EnumerableConverter.Instance);
        }
    }
}
