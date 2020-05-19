using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Serialization;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Json.Serialization;
using Raven.Client.Json.Serialization.JsonNet;
using Raven.Client.Json.Serialization.JsonNet.Internal;
using Raven.Client.Json.Serialization.JsonNet.Internal.Converters;
using Sparrow.Json;

namespace Raven.Client.Newtonsoft.Json
{
    public class JsonNetSerializationConventions : ISerializationConventions
    {
        private DocumentConventions _parent;

        private BlittableJsonConverter _defaultConverter;
        private IContractResolver _jsonContractResolver;
        private Action<JsonSerializer> _customizeJsonSerializer;
        private Action<JsonSerializer> _customizeJsonDeserializer;
        private Func<Type, BlittableJsonReaderObject, object> _deserializeEntityFromBlittable;
        private JsonEnumerableConverter _jsonEnumerableConverter;

        /// <summary>
        ///     Register an action to customize the json serializer used by the <see cref="DocumentStore" />
        /// </summary>
        public Action<JsonSerializer> CustomizeJsonSerializer
        {
            get => _customizeJsonSerializer;
            set
            {
                _parent.AssertNotFrozen();
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
                _parent.AssertNotFrozen();
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
                _parent.AssertNotFrozen();
                _jsonContractResolver = value;
            }
        }

        public Func<Type, BlittableJsonReaderObject, object> DeserializeEntityFromBlittable
        {
            get => _deserializeEntityFromBlittable;
            set
            {
                _parent.AssertNotFrozen();
                _deserializeEntityFromBlittable = value;
            }
        }

        void ISerializationConventions.Freeze(DocumentConventions conventions)
        {
            _parent = conventions ?? throw new ArgumentNullException(nameof(conventions));

            _defaultConverter = new BlittableJsonConverter(conventions);
            _jsonEnumerableConverter = new JsonEnumerableConverter(conventions);

            DeserializeEntityFromBlittable = new JsonNetBlittableEntitySerializer(conventions, this).EntityFromJsonStream;
            JsonContractResolver = new DefaultRavenContractResolver(conventions);
            CustomizeJsonSerializer = _ => { };
            CustomizeJsonDeserializer = _ => { };
        }

        IBlittableJsonConverter ISerializationConventions.DefaultConverter => _defaultConverter;

        ISessionBlittableJsonConverter ISerializationConventions.CreateConverter(InMemoryDocumentSessionOperations session)
        {
            return new SessionBlittableJsonConverter(session);
        }

        IJsonSerializer ISerializationConventions.CreateDeserializer()
        {
            var jsonSerializer = CreateInitialSerializer();
            CustomizeJsonSerializer(jsonSerializer);
            CustomizeJsonDeserializer(jsonSerializer);
            PostJsonSerializerInitiation(jsonSerializer);
            return jsonSerializer;
        }

        IJsonSerializer ISerializationConventions.CreateSerializer()
        {
            var jsonSerializer = CreateInitialSerializer();
            CustomizeJsonSerializer(jsonSerializer);
            PostJsonSerializerInitiation(jsonSerializer);
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

        private JsonNetJsonSerializer CreateInitialSerializer()
        {
            return new JsonNetJsonSerializer
            {
                DateParseHandling = DateParseHandling.None,
                ObjectCreationHandling = ObjectCreationHandling.Auto,
                ContractResolver = JsonContractResolver,
                TypeNameHandling = TypeNameHandling.Auto,
                TypeNameAssemblyFormatHandling = TypeNameAssemblyFormatHandling.Simple,
                ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor,
                FloatParseHandling = FloatParseHandling.Double
            };
        }

        private void PostJsonSerializerInitiation(JsonNetJsonSerializer jsonSerializer)
        {
            if (_parent.SaveEnumsAsIntegers == false)
                jsonSerializer.Converters.Add(new StringEnumConverter());

            jsonSerializer.Converters.Add(JsonDateTimeISO8601Converter.Instance);
            jsonSerializer.Converters.Add(JsonLuceneDateTimeConverter.Instance);
            jsonSerializer.Converters.Add(JsonDictionaryDateTimeKeysConverter.Instance);
            jsonSerializer.Converters.Add(ParametersConverter.Instance);
            jsonSerializer.Converters.Add(JsonLinqEnumerableConverter.Instance);
            jsonSerializer.Converters.Add(JsonIMetadataDictionaryConverter.Instance);
            jsonSerializer.Converters.Add(SizeConverter.Instance);
            jsonSerializer.Converters.Add(_jsonEnumerableConverter);
        }
    }
}
