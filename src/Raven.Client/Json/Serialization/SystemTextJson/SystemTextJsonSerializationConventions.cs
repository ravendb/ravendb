using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Dynamic;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Json.Serialization.SystemTextJson.Internal;
using Raven.Client.Json.Serialization.SystemTextJson.Internal.Converters;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.SystemTextJson
{
    public class SystemTextJsonSerializationConventions : ISerializationConventions, IJsonPropertyNameResolver, IEntityStreamWriter
    {
        internal static readonly JsonWriterOptions WriterOptions = new() { Encoder = SupplementaryPlaneJavaScriptEncoder.Instance };

        internal static readonly JsonWriterOptions MetadataWriterOptions = new() { Encoder = SupplementaryPlaneJavaScriptEncoder.Instance, SkipValidation = true };

        private readonly BlittableJsonConverter _defaultConverter;
        private readonly ConcurrentDictionary<Type, JsonPropertyMap> _jsonProperties = new();
        private readonly ConcurrentDictionary<Type, MissingPropertiesShape> _missingPropertiesShapes = new();
        private Action<JsonSerializerOptions> _customizeJsonSerializerOptions;
        private Func<Type, BlittableJsonReaderObject, object> _deserializeEntityFromBlittable;
        private bool _ignoreByRefMembers;
        private bool _ignoreUnsafeMembers;
        private bool _unboundedReadDepth;
        private JsonSerializerContext _sourceGenerationContext;
        private readonly JsonSerializerOptions[] _options = new JsonSerializerOptions[3];

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

        internal bool UnboundedReadDepth
        {
            get => _unboundedReadDepth;
            set
            {
                Conventions?.AssertNotFrozen();
                _unboundedReadDepth = value;
            }
        }

        internal JsonReaderOptions GetReaderOptions(JsonSerializerOptions options)
        {
            return new JsonReaderOptions
            {
                AllowTrailingCommas = options.AllowTrailingCommas,
                CommentHandling = options.ReadCommentHandling,
                MaxDepth = _unboundedReadDepth ? int.MaxValue : options.MaxDepth
            };
        }

        internal JsonSerializerOptions Options => GetOptions(TypeNameHandling.Auto);

        private JsonSerializerOptions GetOptions(TypeNameHandling typeNameHandling)
        {
            JsonSerializerOptions options = _options[(int)typeNameHandling];
            if (options != null)
                return options;

            options = CreateJsonSerializerOptions(typeNameHandling);
            _options[(int)typeNameHandling] = options;
            return options;
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
            TypeNameHandling typeNameHandling = options?.TypeNameHandling ?? TypeNameHandling.Auto;
            JsonSerializerOptions serializerOptions = GetOptions(typeNameHandling);
            return new SystemTextJsonJsonSerializer(serializerOptions, GetReaderOptions(serializerOptions), typeNameHandling);
        }

        IJsonSerializer ISerializationConventions.CreateSerializer(CreateSerializerOptions options)
        {
            TypeNameHandling typeNameHandling = options?.TypeNameHandling ?? TypeNameHandling.Auto;
            JsonSerializerOptions serializerOptions = GetOptions(typeNameHandling);
            return new SystemTextJsonJsonSerializer(serializerOptions, GetReaderOptions(serializerOptions), typeNameHandling);
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

        string IJsonPropertyNameResolver.GetJsonPropertyName(MemberInfo member)
        {
            return JsonPropertyNameResolver.GetSystemTextJsonPropertyName(member);
        }

        internal JsonPropertyMap GetJsonProperties(Type type)
        {
            return _jsonProperties.TryGetValue(type, out JsonPropertyMap properties)
                ? properties
                : _jsonProperties.GetOrAdd(type, t => new JsonPropertyMap(RavenJsonTypeInfoResolver.GetObjectTypeInfo(Options, t), Options.PropertyNameCaseInsensitive));
        }

        internal MissingPropertiesShape GetMissingPropertiesShape(Type type)
        {
            return _missingPropertiesShapes.TryGetValue(type, out MissingPropertiesShape shape)
                ? shape
                : _missingPropertiesShapes.GetOrAdd(type, t => new MissingPropertiesShape(RavenJsonTypeInfoResolver.GetObjectTypeInfo(Options, t), GetJsonProperties(t)));
        }

        private JsonSerializerOptions CreateJsonSerializerOptions(TypeNameHandling typeNameHandling)
        {
            var ravenResolver = new RavenJsonTypeInfoResolver(this, typeNameHandling);

            var options = new JsonSerializerOptions
            {
                TypeInfoResolver = _sourceGenerationContext != null
                    ? ravenResolver.WithSourceGenerationContext(_sourceGenerationContext)
                    : ravenResolver,
                NumberHandling = JsonNumberHandling.AllowReadingFromString,
                Encoder = SupplementaryPlaneJavaScriptEncoder.Instance,
                IncludeFields = true,
                PropertyNameCaseInsensitive = true,
                DefaultIgnoreCondition = JsonIgnoreCondition.Never
            };

            CustomizeJsonSerializerOptions(options);
            InitializeConverters(options, typeNameHandling);

            if (options.PropertyNamingPolicy != null && Conventions?.PropertyNameConverter == null)
                ThrowPropertyNamingPolicyNotSupported();

            return options;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowPropertyNamingPolicyNotSupported() =>
            throw new NotSupportedException(
                $"{nameof(JsonSerializerOptions)}.{nameof(JsonSerializerOptions.PropertyNamingPolicy)} renames document fields, while queries, indexes and patches address them by member name. " +
                $"Set {nameof(DocumentConventions)}.{nameof(DocumentConventions.PropertyNameConverter)} instead, which RavenDB applies to both.");

        bool IEntityStreamWriter.TryWriteEntityToStream(Stream stream, object entity, IMetadataDictionary metadata, IJsonSerializer serializer, JsonOperationContext context)
        {
            if (entity is IDynamicMetaObjectProvider || serializer is not SystemTextJsonJsonSerializer stjSerializer)
                return false;

            Type type = entity.GetType();
            bool hasIdentityProperty = Conventions.GetIdentityProperty(type) != null;

            using (RavenJsonTypeInfoResolver.EnterRoot(entity, removeIdentityProperty: true, hasIdentityProperty))
            using (ContextMemoryBufferWriter buffer = stjSerializer.SerializeToBuffer(entity, type, metadata, documentInfo: null, context))
            {
                if (hasIdentityProperty && RavenJsonTypeInfoResolver.RemovedIdentityProperty == false)
                    return false;

                stream.Write(buffer.WrittenSpan);
            }

            return true;
        }

        internal static void WriteMetadata(Utf8JsonWriter writer, IEnumerable<KeyValuePair<string, object>> metadata)
        {
            var sink = new Utf8JsonMetadataValueSink(writer);
            MetadataValueWriter.WriteDictionary(ref sink, metadata);
        }

        private void InitializeConverters(JsonSerializerOptions options, TypeNameHandling typeNameHandling)
        {
            if (Conventions == null || Conventions.SaveEnumsAsIntegers == false)
                options.Converters.Add(EnumMemberStringEnumConverter.Instance);
            else
                options.Converters.Add(IntegerEnumConverter.Instance);

            foreach (JsonConverter converter in IntegralNumberConverters.All)
                options.Converters.Add(converter);

            options.Converters.Add(EmptyStringNullableConverter.Instance);
            options.Converters.Add(CharConverter.Instance);
            options.Converters.Add(StringConverter.Instance);
            options.Converters.Add(BooleanConverter.Instance);

            options.Converters.Add(FloatingPointConverters.Double);
            options.Converters.Add(FloatingPointConverters.Single);
            options.Converters.Add(FloatingPointConverters.Decimal);
            options.Converters.Add(DateTimeISO8601Converter.Instance);
            options.Converters.Add(GuidConverter.Instance);
            options.Converters.Add(VersionConverter.Instance);
            options.Converters.Add(UIntPtrConverter.Instance);
            options.Converters.Add(new ParametersConverter(() => GetOptions(TypeNameHandling.None)));
            options.Converters.Add(LinqEnumerableConverter.Instance);
            options.Converters.Add(MetadataDictionaryConverter.Instance);
            options.Converters.Add(SizeConverter.Instance);
#if FEATURE_DATEONLY_TIMEONLY_SUPPORT
            options.Converters.Add(DateOnlyConverter.Instance);
            options.Converters.Add(TimeOnlyConverter.Instance);
#endif
            options.Converters.Add(VectorConverter.Instance);
            options.Converters.Add(new EnumerableConstructorConverter(typeNameHandling));
            options.Converters.Add(JsonDynamicConverters.Value);
            options.Converters.Add(BigIntegerConverter.Instance);
            options.Converters.Add(StringContractConverterFactory.Instance);
            options.Converters.Add(new ObjectConverter(typeNameHandling));
        }
    }
}
