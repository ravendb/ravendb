using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Microsoft.CSharp.RuntimeBinder;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Serialization;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Conventions
{
    /// <summary>
    ///     The set of conventions used by the <see cref="DocumentStore" /> which allow the users to customize
    ///     the way the Raven client API behaves
    /// </summary>
    public class DocumentConventions : Client.Conventions
    {
        public delegate LinqPathProvider.Result CustomQueryTranslator(LinqPathProvider provider, Expression expression);

        public delegate bool TryConvertValueForQueryDelegate<in T>(string fieldName, T value, bool forRange, out string strValue);

        internal static DocumentConventions Default = new DocumentConventions();

        private static IDictionary<Type, string> _cachedDefaultTypeCollectionNames = new Dictionary<Type, string>();

        private readonly Dictionary<MemberInfo, CustomQueryTranslator> _customQueryTranslators = new Dictionary<MemberInfo, CustomQueryTranslator>();

        private readonly List<(Type Type, TryConvertValueForQueryDelegate<object> Convert)> _listOfQueryValueConverters = new List<(Type, TryConvertValueForQueryDelegate<object>)>();

        private readonly IList<Tuple<Type, Func<string, object, Task<string>>>> _listOfRegisteredIdConventionsAsync = new List<Tuple<Type, Func<string, object, Task<string>>>>();

        private readonly IList<Tuple<Type, Func<ValueType, string>>> _listOfRegisteredIdLoadConventions = new List<Tuple<Type, Func<ValueType, string>>>();

        /// <summary>
        ///     Initializes a new instance of the <see cref="DocumentConventions" /> class.
        /// </summary>
        public DocumentConventions()
        {
            ReadBalanceBehavior = ReadBalanceBehavior.None;

            FindIdentityProperty = q => q.Name == "Id";
            IdentityPartsSeparator = "/";
            FindIdentityPropertyNameFromEntityName = entityName => "Id";

            FindClrType = (id, doc) =>
            {
                if (doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) && metadata.TryGet(Constants.Documents.Metadata.RavenClrType, out string clrType))
                    return clrType;

                return null;
            };
            FindClrTypeName = ReflectionUtil.GetFullNameWithoutVersionInformation;

            TransformTypeCollectionNameToDocumentIdPrefix = DefaultTransformCollectionNameToDocumentIdPrefix;
            FindCollectionName = DefaultGetCollectionName;

            FindPropertyNameForIndex = (indexedType, indexedName, path, prop) => (path + prop).Replace("[].", "_").Replace(".", "_");
            FindPropertyNameForDynamicIndex = (indexedType, indexedName, path, prop) => path + prop;

            MaxNumberOfRequestsPerSession = 30;

            PrettifyGeneratedLinqExpressions = true;

            JsonContractResolver = new DefaultRavenContractResolver();
            CustomizeJsonSerializer = serializer => { }; // todo: remove this or merge with SerializeEntityToJsonStream
            SerializeEntityToJsonStream = (entity, streamWriter) =>
            {
                var jsonSerializer = CreateSerializer();
                jsonSerializer.Serialize(streamWriter, entity);
                streamWriter.Flush();
            };

            DeserializeEntityFromBlittable = new JsonNetBlittableEntitySerializer(this).EntityFromJsonStream;
        }

        private bool _frozen;
        private ClientConfiguration _originalConfiguration;
        private Dictionary<Type, MemberInfo> _idPropertyCache = new Dictionary<Type, MemberInfo>();

        private bool _saveEnumsAsIntegers;
        private string _identityPartsSeparator;
        private bool _disableTopologyUpdates;
        private Func<MemberInfo, bool> _findIdentityProperty;
        private bool _prettifyGeneratedLinqExpressions;
        private Func<string, string> _transformTypeCollectionNameToDocumentIdPrefix;
        private Func<string, object, Task<string>> _asyncDocumentIdGenerator;
        private Func<string, string> _findIdentityPropertyNameFromEntityName;
        private Func<Type, string, string, string, string> _findPropertyNameForDynamicIndex;
        private Func<Type, string, string, string, string> _findPropertyNameForIndex;
        private Func<dynamic, string> _findCollectionNameForDynamic;
        private Func<Type, string> _findCollectionName;
        private IContractResolver _jsonContractResolver;
        private Func<Type, string> _findClrTypeName;
        private Func<string, BlittableJsonReaderObject, string> _findClrType;
        private bool _useOptimisticConcurrency;
        private bool _throwIfQueryPageSizeIsNotSet;
        private int _maxNumberOfRequestsPerSession;
        private Action<JsonSerializer> _customizeJsonSerializer;
        private Action<object, StreamWriter> _serializeEntityToJsonStream;
        private ReadBalanceBehavior _readBalanceBehavior;
        private Func<Type, BlittableJsonReaderObject, object> _deserializeEntityFromBlittable;

        public Func<Type, BlittableJsonReaderObject, object> DeserializeEntityFromBlittable
        {
            get => _deserializeEntityFromBlittable;
            set
            {
                AssertNotFrozen();
                _deserializeEntityFromBlittable = value;
            }
        }

        public ReadBalanceBehavior ReadBalanceBehavior
        {
            get => _readBalanceBehavior;
            set
            {
                AssertNotFrozen();
                _readBalanceBehavior = value;
            }
        }

        public Action<object, StreamWriter> SerializeEntityToJsonStream
        {
            get => _serializeEntityToJsonStream;
            set
            {
                AssertNotFrozen();
                _serializeEntityToJsonStream = value;
            }
        }

        /// <summary>
        ///     Register an action to customize the json serializer used by the <see cref="DocumentStore" />
        /// </summary>
        public Action<JsonSerializer> CustomizeJsonSerializer
        {
            get => _customizeJsonSerializer;
            set
            {
                AssertNotFrozen();
                _customizeJsonSerializer = value;
            }
        }

        /// <summary>
        ///     Gets or sets the max length of Url of GET requests.
        /// </summary>
        /// <value>The max number of requests per session.</value>
        public int MaxNumberOfRequestsPerSession
        {
            get => _maxNumberOfRequestsPerSession;
            set
            {
                AssertNotFrozen();
                _maxNumberOfRequestsPerSession = value;
            }
        }

        /// <summary>
        ///     If set to 'true' then it will throw an exception when any query is performed (in session)
        ///     without explicit page size set.
        ///     This can be useful for development purposes to pinpoint all the possible performance bottlenecks
        ///     since from 4.0 there is no limitation for number of results returned from server.
        /// </summary>
        public bool ThrowIfQueryPageSizeIsNotSet
        {
            get => _throwIfQueryPageSizeIsNotSet;
            set
            {
                AssertNotFrozen();
                _throwIfQueryPageSizeIsNotSet = value;
            }
        }

        /// <summary>
        ///     Whether UseOptimisticConcurrency is set to true by default for all opened sessions
        /// </summary>
        public bool UseOptimisticConcurrency
        {
            get => _useOptimisticConcurrency;
            set
            {
                AssertNotFrozen();
                _useOptimisticConcurrency = value;
            }
        }

        /// <summary>
        ///     Gets or sets the function to find the clr type of a document.
        /// </summary>
        public Func<string, BlittableJsonReaderObject, string> FindClrType
        {
            get => _findClrType;
            set
            {
                AssertNotFrozen();
                _findClrType = value;
            }
        }

        /// <summary>
        ///     Gets or sets the function to find the clr type name from a clr type
        /// </summary>
        public Func<Type, string> FindClrTypeName
        {
            get => _findClrTypeName;
            set
            {
                AssertNotFrozen();
                _findClrTypeName = value;
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
                AssertNotFrozen();
                _jsonContractResolver = value;
            }
        }

        /// <summary>
        ///     Gets or sets the function to find the collection name for given type.
        /// </summary>
        public Func<Type, string> FindCollectionName
        {
            get => _findCollectionName;
            set
            {
                AssertNotFrozen();
                _findCollectionName = value;
            }
        }

        /// <summary>
        ///     Gets or sets the function to find the collection name for dynamic type.
        /// </summary>
        public Func<dynamic, string> FindCollectionNameForDynamic
        {
            get => _findCollectionNameForDynamic;
            set
            {
                AssertNotFrozen();
                _findCollectionNameForDynamic = value;
            }
        }

        /// <summary>
        ///     Gets or sets the function to find the indexed property name
        ///     given the indexed document type, the index name, the current path and the property path.
        /// </summary>
        public Func<Type, string, string, string, string> FindPropertyNameForIndex
        {
            get => _findPropertyNameForIndex;
            set
            {
                AssertNotFrozen();
                _findPropertyNameForIndex = value;
            }
        }

        /// <summary>
        ///     Gets or sets the function to find the indexed property name
        ///     given the indexed document type, the index name, the current path and the property path.
        /// </summary>
        public Func<Type, string, string, string, string> FindPropertyNameForDynamicIndex
        {
            get => _findPropertyNameForDynamicIndex;
            set
            {
                AssertNotFrozen();
                _findPropertyNameForDynamicIndex = value;
            }
        }

        /// <summary>
        ///     Get or sets the function to get the identity property name from the entity name
        /// </summary>
        public Func<string, string> FindIdentityPropertyNameFromEntityName
        {
            get => _findIdentityPropertyNameFromEntityName;
            set
            {
                AssertNotFrozen();
                _findIdentityPropertyNameFromEntityName = value;
            }
        }

        /// <summary>
        ///     Gets or sets the document ID generator.
        /// </summary>
        /// <value>The document ID generator.</value>
        public Func<string, object, Task<string>> AsyncDocumentIdGenerator
        {
            get => _asyncDocumentIdGenerator;
            set
            {
                AssertNotFrozen();
                _asyncDocumentIdGenerator = value;
            }
        }

        /// <summary>
        ///     Translates the types collection name to the document id prefix
        /// </summary>
        public Func<string, string> TransformTypeCollectionNameToDocumentIdPrefix
        {
            get => _transformTypeCollectionNameToDocumentIdPrefix;
            set
            {
                AssertNotFrozen();
                _transformTypeCollectionNameToDocumentIdPrefix = value;
            }
        }

        /// <summary>
        ///     Attempts to prettify the generated linq expressions for indexes and transformers
        /// </summary>
        public bool PrettifyGeneratedLinqExpressions
        {
            get => _prettifyGeneratedLinqExpressions;
            set
            {
                AssertNotFrozen();
                _prettifyGeneratedLinqExpressions = value;
            }
        }

        /// <summary>
        ///     Gets or sets the function to find the identity property.
        /// </summary>
        /// <value>The find identity property.</value>
        public Func<MemberInfo, bool> FindIdentityProperty
        {
            get => _findIdentityProperty;
            set
            {
                AssertNotFrozen();
                _findIdentityProperty = value;
            }
        }

        public bool DisableTopologyUpdates
        {
            get => _disableTopologyUpdates;
            set
            {
                AssertNotFrozen();
                _disableTopologyUpdates = value;
            }
        }

        /// <summary>
        ///     Gets or sets the identity parts separator used by the HiLo generators
        /// </summary>
        /// <value>The identity parts separator.</value>
        public string IdentityPartsSeparator
        {
            get => _identityPartsSeparator;
            set
            {
                AssertNotFrozen();
                _identityPartsSeparator = value;
            }
        }

        /// <summary>
        ///     Saves Enums as integers and instruct the Linq provider to query enums as integer values.
        /// </summary>
        public bool SaveEnumsAsIntegers
        {
            get => _saveEnumsAsIntegers;
            set
            {
                AssertNotFrozen();
                _saveEnumsAsIntegers = value;
            }
        }

        public void RegisterCustomQueryTranslator<T>(Expression<Func<T, object>> member, CustomQueryTranslator translator)
        {
            AssertNotFrozen();

            var body = member.Body as UnaryExpression;
            if (body == null)
                throw new NotSupportedException("A custom query translator can only be used to evaluate a simple member access or method call.");

            var info = GetMemberInfoFromExpression(body.Operand);

            if (_customQueryTranslators.ContainsKey(info) == false)
                _customQueryTranslators.Add(info, translator);
        }

        /// <summary>
        ///     Default method used when finding a collection name for a type
        /// </summary>
        public static string DefaultGetCollectionName(Type t)
        {
            if (_cachedDefaultTypeCollectionNames.TryGetValue(t, out var result))
                return result;

            if (t.Name.Contains("<>"))
                return null;
            if (t.GetTypeInfo().IsGenericType)
            {
                var name = t.GetGenericTypeDefinition().Name;
                if (name.Contains('`'))
                    name = name.Substring(0, name.IndexOf('`'));
                var sb = new StringBuilder(Inflector.Pluralize(name));
                foreach (var argument in t.GetGenericArguments())
                    sb.Append("Of")
                        .Append(DefaultGetCollectionName(argument));
                result = sb.ToString();
            }
            else if (t == typeof(object))
            {
                result = Constants.Documents.Collections.AllDocumentsCollection;
            }
            else
            {
                result = Inflector.Pluralize(t.Name);
            }
            var temp = new Dictionary<Type, string>(_cachedDefaultTypeCollectionNames)
            {
                [t] = result
            };

            _cachedDefaultTypeCollectionNames = temp;
            return result;
        }

        /// <summary>
        ///     Gets the collection name for a given type.
        /// </summary>
        public string GetCollectionName(Type type)
        {
            return FindCollectionName(type) ?? DefaultGetCollectionName(type);
        }

        /// <summary>
        ///     Gets the collection name for a given dynamic type.
        /// </summary>
        public string GetCollectionName(object entity)
        {
            if (entity == null)
                return null;

            if (FindCollectionNameForDynamic != null && entity is IDynamicMetaObjectProvider)
                try
                {
                    return FindCollectionNameForDynamic(entity);
                }
                catch (RuntimeBinderException)
                {
                    // if we can't find it, we'll just assume the the propery
                    // isn't there
                }

            return GetCollectionName(entity.GetType());
        }

        /// <summary>
        ///     Generates the document id.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <param name="databaseName">Name of the database</param>
        /// <returns></returns>
        public string GenerateDocumentId(string databaseName, object entity)
        {
            return AsyncHelpers.RunSync(() => GenerateDocumentIdAsync(databaseName, entity));
        }

        public Task<string> GenerateDocumentIdAsync(string databaseName, object entity)
        {
            var type = entity.GetType();
            foreach (var typeToRegisteredIdConvention in _listOfRegisteredIdConventionsAsync
                .Where(typeToRegisteredIdConvention => typeToRegisteredIdConvention.Item1.IsAssignableFrom(type)))
                return typeToRegisteredIdConvention.Item2(databaseName, entity);

            return AsyncDocumentIdGenerator(databaseName, entity);
        }

        /// <summary>
        ///     Register an async id convention for a single type (and all of its derived types.
        ///     Note that you can still fall back to the DocumentIdGenerator if you want.
        /// </summary>
        public DocumentConventions RegisterAsyncIdConvention<TEntity>(Func<string, TEntity, Task<string>> func)
        {
            AssertNotFrozen();

            var type = typeof(TEntity);
            var entryToRemove = _listOfRegisteredIdConventionsAsync.FirstOrDefault(x => x.Item1 == type);
            if (entryToRemove != null)
                _listOfRegisteredIdConventionsAsync.Remove(entryToRemove);

            int index;
            for (index = 0; index < _listOfRegisteredIdConventionsAsync.Count; index++)
            {
                var entry = _listOfRegisteredIdConventionsAsync[index];
                if (entry.Item1.IsAssignableFrom(type))
                    break;
            }

            var item = new Tuple<Type, Func<string, object, Task<string>>>(typeof(TEntity), (dbName, o) => func(dbName, (TEntity)o));
            _listOfRegisteredIdConventionsAsync.Insert(index, item);

            return this;
        }

        /// <summary>
        ///     Register an id convention for a single type (and all its derived types) to be used when calling
        ///     session.Load{TEntity}(TId id)
        ///     It is used by the default implementation of FindFullDocumentIdFromNonStringIdentifier.
        /// </summary>
        public DocumentConventions RegisterIdLoadConvention<TEntity>(Func<ValueType, string> func)
        {
            AssertNotFrozen();

            var type = typeof(TEntity);
            var entryToRemove = _listOfRegisteredIdLoadConventions.FirstOrDefault(x => x.Item1 == type);
            if (entryToRemove != null)
                _listOfRegisteredIdLoadConventions.Remove(entryToRemove);

            int index;
            for (index = 0; index < _listOfRegisteredIdLoadConventions.Count; index++)
            {
                var entry = _listOfRegisteredIdLoadConventions[index];
                if (entry.Item1.IsAssignableFrom(type))
                    break;
            }

            var item = new Tuple<Type, Func<ValueType, string>>(typeof(TEntity), func);
            _listOfRegisteredIdLoadConventions.Insert(index, item);

            return this;
        }


        /// <summary>
        ///     Creates the serializer.
        /// </summary>
        /// <returns></returns>
        public JsonSerializer CreateSerializer()
        {
            var jsonSerializer = new JsonSerializer
            {
                DateParseHandling = DateParseHandling.None,
                ObjectCreationHandling = ObjectCreationHandling.Auto,
                ContractResolver = JsonContractResolver,
                TypeNameHandling = TypeNameHandling.Auto,
                TypeNameAssemblyFormatHandling = TypeNameAssemblyFormatHandling.Simple,
                ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor,
                FloatParseHandling = FloatParseHandling.Double
            };

            CustomizeJsonSerializer(jsonSerializer);
            //TODO - EFRAT
            if (SaveEnumsAsIntegers == false)
                jsonSerializer.Converters.Add(new StringEnumConverter());

            jsonSerializer.Converters.Add(JsonDateTimeISO8601Converter.Instance);
            jsonSerializer.Converters.Add(JsonLuceneDateTimeConverter.Instance);
            jsonSerializer.Converters.Add(JsonObjectConverter.Instance);
            jsonSerializer.Converters.Add(JsonDictionaryDateTimeKeysConverter.Instance);
            jsonSerializer.Converters.Add(ParametersConverter.Instance);
            jsonSerializer.Converters.Add(JsonLinqEnumerableConverter.Instance);
            // TODO: Iftah
            //var convertersToUse = SaveEnumsAsIntegers ? DefaultConvertersEnumsAsIntegers : DefaultConverters;
            //if (jsonSerializer.Converters.Count == 0)
            //{
            //    jsonSerializer.Converters = convertersToUse;
            //}
            //else
            //{
            //    for (int i = convertersToUse.Count - 1; i >= 0; i--)
            //    {
            //        jsonSerializer.Converters.Insert(0, convertersToUse[i]);
            //    }
            //}
            return jsonSerializer;
        }

        /// <summary>
        ///     Get the CLR type (if exists) from the document
        /// </summary>
        public string GetClrType(string id, BlittableJsonReaderObject document)
        {
            return FindClrType(id, document);
        }

        /// <summary>
        ///     Get the CLR type name to be stored in the entity metadata
        /// </summary>
        public string GetClrTypeName(Type entityType)
        {
            return FindClrTypeName(entityType);
        }

        /// <summary>
        ///     Clone the current conventions to a new instance
        /// </summary>
        public DocumentConventions Clone()
        {
            return (DocumentConventions)MemberwiseClone();
        }

        public static RangeType GetRangeType(object o)
        {
            if (o == null)
                return RangeType.None;

            var type = o as Type ?? o.GetType();
            return GetRangeType(type);
        }

        public static RangeType GetRangeType(Type type)
        {
            var nonNullable = Nullable.GetUnderlyingType(type);
            if (nonNullable != null)
                type = nonNullable;

            if (type == typeof(int) || type == typeof(long) || type == typeof(short) || type == typeof(TimeSpan))
                return RangeType.Long;

            if (type == typeof(double) || type == typeof(float) || type == typeof(decimal))
                return RangeType.Double;

            return RangeType.None;
        }

        /// <summary>
        ///     Gets the identity property.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        public MemberInfo GetIdentityProperty(Type type)
        {
            var currentIdPropertyCache = _idPropertyCache;
            if (currentIdPropertyCache.TryGetValue(type, out var info))
                return info;

            var identityProperty = GetPropertiesForType(type).FirstOrDefault(FindIdentityProperty);

            if (identityProperty != null && identityProperty.DeclaringType != type)
            {
                var propertyInfo = identityProperty.DeclaringType.GetProperty(identityProperty.Name);
                identityProperty = propertyInfo ?? identityProperty;
            }

            _idPropertyCache = new Dictionary<Type, MemberInfo>(currentIdPropertyCache)
            {
                {type, identityProperty}
            };

            return identityProperty;
        }

        internal void UpdateFrom(ClientConfiguration configuration)
        {
            if (configuration == null)
                return;

            lock (this)
            {
                if (configuration.Disabled && _originalConfiguration == null) // nothing to do
                    return;

                if (configuration.Disabled && _originalConfiguration != null) // need to revert to original values
                {
                    _maxNumberOfRequestsPerSession = _originalConfiguration.MaxNumberOfRequestsPerSession.Value;
                    _prettifyGeneratedLinqExpressions = _originalConfiguration.PrettifyGeneratedLinqExpressions.Value;
                    _readBalanceBehavior = _originalConfiguration.ReadBalanceBehavior.Value;

                    _originalConfiguration = null;
                    return;
                }

                if (_originalConfiguration == null)
                    _originalConfiguration = new ClientConfiguration
                    {
                        Etag = -1,
                        MaxNumberOfRequestsPerSession = MaxNumberOfRequestsPerSession,
                        PrettifyGeneratedLinqExpressions = PrettifyGeneratedLinqExpressions,
                        ReadBalanceBehavior = ReadBalanceBehavior
                    };

                _maxNumberOfRequestsPerSession = configuration.MaxNumberOfRequestsPerSession ?? _originalConfiguration.MaxNumberOfRequestsPerSession.Value;
                _prettifyGeneratedLinqExpressions = configuration.PrettifyGeneratedLinqExpressions ?? _originalConfiguration.PrettifyGeneratedLinqExpressions.Value;
                _readBalanceBehavior = configuration.ReadBalanceBehavior ?? _originalConfiguration.ReadBalanceBehavior.Value;
            }
        }

        public static string DefaultTransformCollectionNameToDocumentIdPrefix(string collectionName)
        {
            var count = collectionName.Count(char.IsUpper);

            if (count <= 1) // simple name, just lower case it
                return collectionName.ToLowerInvariant();

            // multiple capital letters, so probably something that we want to preserve caps on.
            return collectionName;
        }

        private static IEnumerable<MemberInfo> GetPropertiesForType(Type type)
        {
            foreach (var propertyInfo in ReflectionUtil.GetPropertiesAndFieldsFor(type, BindingFlags.Public | BindingFlags.Instance | BindingFlags.NonPublic))
                yield return propertyInfo;

            foreach (var @interface in type.GetInterfaces())
                foreach (var propertyInfo in GetPropertiesForType(@interface))
                    yield return propertyInfo;
        }

        public void RegisterQueryValueConverter<T>(TryConvertValueForQueryDelegate<T> converter)
        {
            AssertNotFrozen();

            int index;
            for (index = 0; index < _listOfQueryValueConverters.Count; index++)
            {
                var entry = _listOfQueryValueConverters[index];
                if (entry.Type.IsAssignableFrom(typeof(T)))
                    break;
            }

            _listOfQueryValueConverters.Insert(index, (typeof(T), Actual));

            bool Actual(string name, object value, bool forRange, out string strValue)
            {
                if (value is T)
                    return converter(name, (T)value, forRange, out strValue);
                strValue = null;
                return false;
            }
        }

        public bool TryConvertValueForQuery(string fieldName, object value, bool forRange, out string strValue)
        {
            foreach (var queryValueConverter in _listOfQueryValueConverters)
            {
                if (queryValueConverter.Type.IsInstanceOfType(value) == false)
                    continue;

                return queryValueConverter.Convert(fieldName, value, forRange, out strValue);
            }

            strValue = null;
            return false;
        }

        internal LinqPathProvider.Result TranslateCustomQueryExpression(LinqPathProvider provider, Expression expression)
        {
            var member = GetMemberInfoFromExpression(expression);

            return _customQueryTranslators.TryGetValue(member, out var translator) == false
                ? null
                : translator.Invoke(provider, expression);
        }

        private static MemberInfo GetMemberInfoFromExpression(Expression expression)
        {
            var callExpression = expression as MethodCallExpression;
            if (callExpression != null)
                return callExpression.Method;

            var memberExpression = expression as MemberExpression;
            if (memberExpression != null)
                return memberExpression.Member;

            throw new NotSupportedException("A custom query translator can only be used to evaluate a simple member access or method call.");
        }

        internal void Freeze()
        {
            _frozen = true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AssertNotFrozen()
        {
            if (_frozen)
                throw new InvalidOperationException($"Conventions has frozen after '{nameof(DocumentStore)}.{nameof(DocumentStore.Initialize)}()' and no changes can be applied to them.");
        }
    }
}
