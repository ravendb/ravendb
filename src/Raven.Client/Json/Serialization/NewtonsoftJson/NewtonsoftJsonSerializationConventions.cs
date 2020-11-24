using System;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Serialization;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Json.Serialization.NewtonsoftJson.Internal;
using Raven.Client.Json.Serialization.NewtonsoftJson.Internal.Converters;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.NewtonsoftJson
{
    public class NewtonsoftJsonSerializationConventions : ISerializationConventions
    {
        private BlittableJsonConverter _defaultConverter;
        private IContractResolver _jsonContractResolver;
        private Action<JsonSerializer> _customizeJsonSerializer;
        private Action<JsonSerializer> _customizeJsonDeserializer;
        private Func<Type, BlittableJsonReaderObject, object> _deserializeEntityFromBlittable;
        private JsonEnumerableConverter _jsonEnumerableConverter;
        private bool _ignoreByRefMembers;
        private bool _ignoreUnsafeMembers;
        private CachingJsonConverter _cachedConverterSerializer, _cachedConverterDeserializer;

        public DocumentConventions Conventions { get; private set; }

        public NewtonsoftJsonSerializationConventions()
        {
            _defaultConverter = new BlittableJsonConverter(this);
            _jsonEnumerableConverter = new JsonEnumerableConverter(this);
            JsonContractResolver = new DefaultRavenContractResolver(this);
            _ignoreByRefMembers = false;
            _ignoreUnsafeMembers = false;
            CustomizeJsonSerializer = _ => { };
            CustomizeJsonDeserializer = _ => { };
        }

        /// <summary>
        ///     Register an action to customize the json serializer used by the <see cref="DocumentStore" />
        /// </summary>
        public Action<JsonSerializer> CustomizeJsonSerializer
        {
            get => _customizeJsonSerializer;
            set
            {
                Conventions?.AssertNotFrozen();
                _customizeJsonSerializer = value;
            }
        }

        /// <summary>
        ///     Register an action to customize the json serializer used by the <see cref="DocumentStore" /> for deserializations.
        ///     When creating a JsonSerializer, the CustomizeJsonSerializer is always called before CustomizeJsonDeserializer
        /// </summary>
        public Action<JsonSerializer> CustomizeJsonDeserializer
        {
            get => _customizeJsonDeserializer;
            set
            {
                Conventions?.AssertNotFrozen();
                _customizeJsonDeserializer = value;
            }
        }

        /// <summary>
        ///     Gets or sets the json contract resolver.
        /// </summary>
        /// <value>The json contract resolver.</value>
        public IContractResolver JsonContractResolver
        {
            get => _jsonContractResolver;
            set
            {
                Conventions?.AssertNotFrozen();
                _jsonContractResolver = value;
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

        void ISerializationConventions.Initialize(DocumentConventions conventions)
        {
            Conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));

            if (_deserializeEntityFromBlittable == null)
                _deserializeEntityFromBlittable = new NewtonsoftJsonBlittableEntitySerializer(this).EntityFromJsonStream;
        }

        IBlittableJsonConverter ISerializationConventions.DefaultConverter => _defaultConverter;

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

        ISessionBlittableJsonConverter ISerializationConventions.CreateConverter(InMemoryDocumentSessionOperations session)
        {
            return new SessionBlittableJsonConverter(session);
        }

        IJsonSerializer ISerializationConventions.CreateDeserializer(CreateDeserializerOptions options)
        {
            var jsonSerializer = CreateInitialSerializer();
            CustomizeJsonSerializer(jsonSerializer);
            CustomizeJsonDeserializer(jsonSerializer);
            PostJsonSerializerInitiation(jsonSerializer, true, ref _cachedConverterDeserializer);

            jsonSerializer.ApplyOptions(options);

            return jsonSerializer;
        }

        IJsonSerializer ISerializationConventions.CreateSerializer(CreateSerializerOptions options)
        {
            var jsonSerializer = CreateInitialSerializer();
            CustomizeJsonSerializer(jsonSerializer);
            PostJsonSerializerInitiation(jsonSerializer, false, ref _cachedConverterSerializer);

            jsonSerializer.ApplyOptions(options);

            return jsonSerializer;
        }

        IJsonWriter ISerializationConventions.CreateWriter(JsonOperationContext context)
        {
            return new BlittableJsonWriter(context);
        }

        object ISerializationConventions.DeserializeEntityFromBlittable(Type type, BlittableJsonReaderObject json)
        {
            return DeserializeEntityFromBlittable(type, json);
        }

        T ISerializationConventions.DeserializeEntityFromBlittable<T>(BlittableJsonReaderObject json)
        {
            return (T)DeserializeEntityFromBlittable(typeof(T), json);
        }

        private NewtonsoftJsonJsonSerializer CreateInitialSerializer()
        {
            return new NewtonsoftJsonJsonSerializer
            {
                DateParseHandling = DateParseHandling.None,
                ObjectCreationHandling = ObjectCreationHandling.Auto,
                ContractResolver = JsonContractResolver,
                TypeNameHandling = Newtonsoft.Json.TypeNameHandling.Auto,
                TypeNameAssemblyFormatHandling = TypeNameAssemblyFormatHandling.Simple,
                ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor,
                FloatParseHandling = FloatParseHandling.Double
            };
        }

        private void PostJsonSerializerInitiation(NewtonsoftJsonJsonSerializer jsonSerializer, bool canRead, ref CachingJsonConverter cache)
        {
            if (cache == null)
            {
                cache = InitializeConverters(jsonSerializer, canRead);
            }
            jsonSerializer.Converters.Clear();
            jsonSerializer.Converters.Add(cache);
        }

        private CachingJsonConverter InitializeConverters(NewtonsoftJsonJsonSerializer jsonSerializer, bool canRead)
        {
            if (Conventions.SaveEnumsAsIntegers == false)
                jsonSerializer.Converters.Add(new StringEnumConverter());

            jsonSerializer.Converters.Add(JsonDateTimeISO8601Converter.Instance);
            jsonSerializer.Converters.Add(JsonLuceneDateTimeConverter.Instance);
            jsonSerializer.Converters.Add(JsonDictionaryDateTimeKeysConverter.Instance);
            jsonSerializer.Converters.Add(ParametersConverter.Instance);
            jsonSerializer.Converters.Add(JsonLinqEnumerableConverter.Instance);
            jsonSerializer.Converters.Add(JsonIMetadataDictionaryConverter.Instance);
            jsonSerializer.Converters.Add(SizeConverter.Instance);
            jsonSerializer.Converters.Add(_jsonEnumerableConverter);

            var converters = canRead ? 
                jsonSerializer.Converters.Where(x => x.CanRead).ToArray() : 
                jsonSerializer.Converters.ToArray();

            return new CachingJsonConverter(converters, canRead, true);
        }
    }
}
