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
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Raven.Client.Util;
using Sparrow;
using Sparrow.Json;
using Sparrow.Platform;
using Size = Sparrow.Size;

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

        public delegate bool TryConvertValueToObjectForQueryDelegate<in T>(string fieldName, T value, bool forRange, out object objValue);

        internal static readonly DocumentConventions Default = new DocumentConventions();

        internal static readonly DocumentConventions DefaultForServer = new DocumentConventions
        {
            SendApplicationIdentifier = false,
            MaxContextSizeToKeep = new Size(PlatformDetails.Is32Bits == false ? 8 : 2, SizeUnit.Megabytes)
        };

        private static Dictionary<Type, string> _cachedDefaultTypeCollectionNames = new Dictionary<Type, string>();

        private readonly Dictionary<MemberInfo, CustomQueryTranslator> _customQueryTranslators = new Dictionary<MemberInfo, CustomQueryTranslator>();

        private readonly List<(Type Type, TryConvertValueToObjectForQueryDelegate<object> Convert)> _listOfQueryValueToObjectConverters =
            new List<(Type, TryConvertValueToObjectForQueryDelegate<object>)>();

        private readonly List<QueryMethodConverter> _listOfQueryMethodConverters = new List<QueryMethodConverter>();

        private readonly Dictionary<Type, RangeType> _customRangeTypes = new Dictionary<Type, RangeType>();

        private readonly List<Tuple<Type, Func<string, object, Task<string>>>> _listOfRegisteredIdConventionsAsync =
            new List<Tuple<Type, Func<string, object, Task<string>>>>();

        public readonly BulkInsertConventions BulkInsert;

        public readonly AggressiveCacheConventions AggressiveCache;

        public class AggressiveCacheConventions
        {
            private readonly DocumentConventions _conventions;
            private readonly AggressiveCacheOptions _aggressiveCacheOptions;

            internal AggressiveCacheConventions(DocumentConventions conventions)
            {
                _conventions = conventions;
                _aggressiveCacheOptions = new AggressiveCacheOptions(TimeSpan.FromDays(1), AggressiveCacheMode.TrackChanges);
            }

            public TimeSpan Duration
            {
                get => _aggressiveCacheOptions.Duration;
                set
                {
                    _conventions.AssertNotFrozen();
                    _aggressiveCacheOptions.Duration = value;
                }
            }

            public AggressiveCacheMode Mode
            {
                get => _aggressiveCacheOptions.Mode;
                set
                {
                    _conventions.AssertNotFrozen();
                    _aggressiveCacheOptions.Mode = value;
                }
            }
        }

        public class BulkInsertConventions
        {
            private readonly DocumentConventions _conventions;
            private Func<object, IMetadataDictionary, StreamWriter, bool> _trySerializeEntityToJsonStream;

            public Func<object, IMetadataDictionary, StreamWriter, bool> TrySerializeEntityToJsonStream
            {
                get => _trySerializeEntityToJsonStream;
                set
                {
                    _conventions.AssertNotFrozen();
                    _trySerializeEntityToJsonStream = value;
                }
            }

            internal BulkInsertConventions(DocumentConventions conventions)
            {
                _conventions = conventions;
                TrySerializeEntityToJsonStream = null;
            }
        }

        static DocumentConventions()
        {
            Default.Freeze();
            DefaultForServer.Freeze();
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="DocumentConventions" /> class.
        /// </summary>
        public DocumentConventions()
        {
            _topologyCacheLocation = AppContext.BaseDirectory;

            ReadBalanceBehavior = ReadBalanceBehavior.None;

            FindIdentityProperty = q => q.Name == "Id";
            IdentityPartsSeparator = "/";
            FindIdentityPropertyNameFromCollectionName = collectionName => "Id";

            FindClrType = (id, doc) =>
            {
                if (doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) &&
                    metadata.TryGet(Constants.Documents.Metadata.RavenClrType, out string clrType))
                    return clrType;

                return null;
            };
            FindClrTypeName = ReflectionUtil.GetFullNameWithoutVersionInformation;

            TransformTypeCollectionNameToDocumentIdPrefix = DefaultTransformCollectionNameToDocumentIdPrefix;
            FindCollectionName = DefaultGetCollectionName;

            FindPropertyNameForIndex = (indexedType, indexedName, path, prop) => (path + prop).Replace("[].", "_").Replace(".", "_");
            FindPropertyNameForDynamicIndex = (indexedType, indexedName, path, prop) => path + prop;

            MaxNumberOfRequestsPerSession = 30;

#pragma warning disable CS0618 // Type or member is obsolete
            PrettifyGeneratedLinqExpressions = true;
#pragma warning restore CS0618 // Type or member is obsolete

            JsonContractResolver = new DefaultRavenContractResolver();
            CustomizeJsonSerializer = serializer => { };
            CustomizeJsonDeserializer = serializer => { };

            BulkInsert = new BulkInsertConventions(this);

            DeserializeEntityFromBlittable = new JsonNetBlittableEntitySerializer(this).EntityFromJsonStream;

            PreserveDocumentPropertiesNotFoundOnModel = true;

            var httpCacheSizeInMb = PlatformDetails.Is32Bits ? 32 : 128;
            MaxHttpCacheSize = new Size(httpCacheSizeInMb, SizeUnit.Megabytes);
            HttpVersion = System.Net.HttpVersion.Version11;

            OperationStatusFetchMode = OperationStatusFetchMode.ChangesApi;

            AddIdFieldToDynamicObjects = true;
            AggressiveCache = new AggressiveCacheConventions(this);

            _firstBroadcastAttemptTimeout = TimeSpan.FromSeconds(5);
            _secondBroadcastAttemptTimeout = TimeSpan.FromSeconds(30);

            _waitForIndexesAfterSaveChangesTimeout = TimeSpan.FromSeconds(15);
            _waitForReplicationAfterSaveChangesTimeout = TimeSpan.FromSeconds(15);
            _waitForNonStaleResultsTimeout = TimeSpan.FromSeconds(15);

            _sendApplicationIdentifier = true;
            _maxContextSizeToKeep = PlatformDetails.Is32Bits == false
                ? new Size(1, SizeUnit.Megabytes)
                : new Size(256, SizeUnit.Kilobytes);
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
        private Func<string, string> _findIdentityPropertyNameFromCollectionName;
        private Func<Type, string, string, string, string> _findPropertyNameForDynamicIndex;
        private Func<Type, string, string, string, string> _findPropertyNameForIndex;
        private Func<Type, string, string, string, string> _findProjectedPropertyNameForIndex;

        private Func<dynamic, string> _findCollectionNameForDynamic;
        private Func<dynamic, string> _findClrTypeNameForDynamic;
        private Func<Type, string> _findCollectionName;
        private IContractResolver _jsonContractResolver;
        private Func<Type, string> _findClrTypeName;
        private Func<string, BlittableJsonReaderObject, string> _findClrType;
        private bool _useOptimisticConcurrency;
        private bool _throwIfQueryPageSizeIsNotSet;
        private bool _addIdFieldToDynamicObjects;
        private int _maxNumberOfRequestsPerSession;
        private Action<JsonSerializer> _customizeJsonSerializer;
        private Action<JsonSerializer> _customizeJsonDeserializer;
        private TimeSpan? _requestTimeout;
        private TimeSpan _secondBroadcastAttemptTimeout;
        private TimeSpan _firstBroadcastAttemptTimeout;
        private TimeSpan _waitForIndexesAfterSaveChangesTimeout;
        private TimeSpan _waitForReplicationAfterSaveChangesTimeout;
        private TimeSpan _waitForNonStaleResultsTimeout;

        private ReadBalanceBehavior _readBalanceBehavior;
        private Func<Type, BlittableJsonReaderObject, object> _deserializeEntityFromBlittable;
        private bool _preserveDocumentPropertiesNotFoundOnModel;
        private Size _maxHttpCacheSize;
        private bool? _useCompression;
        private Func<MemberInfo, string> _propertyNameConverter;
        private Func<Type, bool> _typeIsKnownServerSide = _ => false;
        private OperationStatusFetchMode _operationStatusFetchMode;
        private string _topologyCacheLocation;
        private Version _httpVersion;
        private bool _sendApplicationIdentifier;
        private Size _maxContextSizeToKeep;

        public Size MaxContextSizeToKeep
        {
            get => _maxContextSizeToKeep;
            set
            {
                AssertNotFrozen();
                _maxContextSizeToKeep = value;
            }
        }

        /// <summary>
        /// Enables sending a unique application identifier to the RavenDB Server that is used for Client API usage tracking.
        /// It allows RavenDB Server to issue performance hint notifications e.g. during robust topology update requests which could indicate Client API misuse impacting the overall performance
        /// </summary>
        public bool SendApplicationIdentifier
        {
            get => _sendApplicationIdentifier;
            set
            {
                AssertNotFrozen();
                _sendApplicationIdentifier = value;
            }
        }

        public Version HttpVersion
        {
            get => _httpVersion;
            set
            {
                AssertNotFrozen();
                _httpVersion = value;
            }
        }

        public Func<MemberInfo, string> PropertyNameConverter
        {
            get => _propertyNameConverter;
            set
            {
                AssertNotFrozen();
                _propertyNameConverter = value;
            }
        }

        public TimeSpan? RequestTimeout
        {
            get => _requestTimeout;
            set
            {
                AssertNotFrozen();
                _requestTimeout = value;
            }
        }

        /// <summary>
        /// Set the timeout for the second broadcast attempt.
        /// Default: 30 Seconds.
        ///
        /// Upon failure of the first attempt the request executor will resend the command to all nodes simultaneously.
        /// </summary>
        public TimeSpan SecondBroadcastAttemptTimeout
        {
            get => _secondBroadcastAttemptTimeout;
            set
            {
                AssertNotFrozen();
                _secondBroadcastAttemptTimeout = value;
            }
        }

        /// <summary>
        /// Set the timeout for the first broadcast attempt.
        /// Default: 5 Seconds.
        ///
        /// First attempt will send a single request to a selected node.
        /// </summary>
        public TimeSpan FirstBroadcastAttemptTimeout
        {
            get => _firstBroadcastAttemptTimeout;
            set
            {
                AssertNotFrozen();
                _firstBroadcastAttemptTimeout = value;
            }
        }

        /// <summary>
        /// Sets the default timeout for DocumentSession.Advanced.WaitForIndexesAfterSaveChanges method.
        /// Default: 15 Seconds.
        /// </summary>
        public TimeSpan WaitForIndexesAfterSaveChangesTimeout
        {
            get => _waitForIndexesAfterSaveChangesTimeout;
            set
            {
                AssertNotFrozen();
                _waitForIndexesAfterSaveChangesTimeout = value;
            }
        }

        /// <summary>
        /// Sets the default timeout for DocumentSession WaitForNonStaleResults methods.
        /// Default: 15 Seconds.
        /// </summary>
        public TimeSpan WaitForNonStaleResultsTimeout
        {
            get => _waitForNonStaleResultsTimeout;
            set
            {
                AssertNotFrozen();
                _waitForNonStaleResultsTimeout = value;
            }
        }

        /// <summary>
        /// Sets the default timeout for DocumentSession.Advanced.WaitForReplicationAfterSaveChanges method.
        /// Default: 15 Seconds.
        /// </summary>
        public TimeSpan WaitForReplicationAfterSaveChangesTimeout
        {
            get => _waitForReplicationAfterSaveChangesTimeout;
            set
            {
                AssertNotFrozen();
                _waitForReplicationAfterSaveChangesTimeout = value;
            }
        }

        internal bool HasExplicitlySetCompressionUsage => _useCompression.HasValue;

        /// <summary>
        /// Should accept gzip/deflate headers be added to all requests?
        /// </summary>
        public bool UseCompression
        {
            get => _useCompression ?? true;
            set
            {
                AssertNotFrozen();
                _useCompression = value;
            }
        }

        public bool PreserveDocumentPropertiesNotFoundOnModel
        {
            get => _preserveDocumentPropertiesNotFoundOnModel;
            set
            {
                AssertNotFrozen();
                _preserveDocumentPropertiesNotFoundOnModel = value;
            }
        }

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
        ///     Register an action to customize the json serializer used by the <see cref="DocumentStore" /> for deserializations.
        ///     When creating a JsonSerializer, the CustomizeJsonSerializer is always called before CustomizeJsonDeserializer
        /// </summary>
        public Action<JsonSerializer> CustomizeJsonDeserializer
        {
            get => _customizeJsonDeserializer;
            set
            {
                AssertNotFrozen();
                _customizeJsonDeserializer = value;
            }
        }

        /// <summary>
        ///     By default, the field 'Id' field will be added to dynamic objects, this allows to disable this behavior.
        ///     Default value is 'true'
        /// </summary>
        public bool AddIdFieldToDynamicObjects
        {
            get => _addIdFieldToDynamicObjects;
            set
            {
                AssertNotFrozen();
                _addIdFieldToDynamicObjects = value;
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
        ///     Gets or sets the max size of the cache in requestExecutor.
        ///     Default value is 512MB on 64 bits, 32MB on 32 bits
        /// </summary>
        /// <value>The max size of cache in requestExecutor.</value>
        public Size MaxHttpCacheSize
        {
            get => _maxHttpCacheSize;
            set
            {
                AssertNotFrozen();
                _maxHttpCacheSize = value;
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
        ///     Gets or sets the function to find the collection name for dynamic type.
        /// </summary>
        public Func<dynamic, string> FindClrTypeNameForDynamic
        {
            get => _findClrTypeNameForDynamic;
            set
            {
                AssertNotFrozen();
                _findClrTypeNameForDynamic = value;
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
        ///     Gets or sets the function to find the projected property name for index,
        ///     given the indexed document type, the index name, the current path and the property path.
        /// </summary>
        public Func<Type, string, string, string, string> FindProjectedPropertyNameForIndex
        {
            get => _findProjectedPropertyNameForIndex;
            set
            {
                AssertNotFrozen();
                _findProjectedPropertyNameForIndex = value;
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
        ///     Get or sets the function to get the identity property name from the collection name
        /// </summary>
        public Func<string, string> FindIdentityPropertyNameFromCollectionName
        {
            get => _findIdentityPropertyNameFromCollectionName;
            set
            {
                AssertNotFrozen();
                _findIdentityPropertyNameFromCollectionName = value;
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

        public Func<Type, bool> TypeIsKnownServerSide
        {
            get => _typeIsKnownServerSide;
            set
            {
                AssertNotFrozen();
                _typeIsKnownServerSide = value;
            }
        }

        /// <summary>
        ///     Attempts to prettify the generated linq expressions for indexes
        /// </summary>
        [Obsolete("This feature is currently not implemented and does not have any effect on the generated LINQ expressions")]
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

        /// <summary>
        /// Changes the way the Operation is fetching the operation status when waiting for completion
        /// </summary>
        public OperationStatusFetchMode OperationStatusFetchMode
        {
            get => _operationStatusFetchMode;
            set
            {
                AssertNotFrozen();
                _operationStatusFetchMode = value;
            }
        }

        /// <summary>
        /// Changes the location of topology cache files. By default it is set to application base directory (AppContext.BaseDirectory)
        /// </summary>
        public string TopologyCacheLocation
        {
            get => _topologyCacheLocation;
            set
            {
                AssertNotFrozen();

                if (string.IsNullOrWhiteSpace(value))
                    throw new ArgumentNullException(nameof(value));

                var directory = new DirectoryInfo(value);
                if (directory.Exists == false)
                    throw new InvalidOperationException("Topology cache location directory does not exist. Please create the directory first.");

                var path = directory.FullName;

                try
                {
                    // checking write permissions
                    var fileName = Guid.NewGuid().ToString("N");
                    var fullPath = Path.Combine(path, fileName);
                    File.WriteAllText(fullPath, string.Empty);
                    File.Delete(fullPath);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"You do not have write permissions to topology cache at '{path}'. Please see inner exception for more details.", e);
                }

                _topologyCacheLocation = path;
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

            // we want to reject queries and other operations on abstract types, because you usually
            // want to use them for polymorphic queries, and that require the conventions to be
            // applied properly, so we reject the behavior and hint to the user explicitly
            if (t.GetTypeInfo().IsInterface)
                throw new InvalidOperationException("Cannot find collection name for interface " + t.FullName +
                                                    ", only concrete classes are supported. Did you forget to customize Conventions.FindCollectionName?");
            if (t.GetTypeInfo().IsAbstract)
                throw new InvalidOperationException("Cannot find collection name for abstract class " + t.FullName +
                                                    ", only concrete class are supported. Did you forget to customize Conventions.FindCollectionName?");

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
                return null;
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
            {
                try
                {
                    return FindCollectionNameForDynamic(entity);
                }
                catch (RuntimeBinderException)
                {
                    // if we can't find it, we'll just assume that the property
                    // isn't there
                }
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

        private JsonSerializer CreateInitialSerializer()
        {
            return new JsonSerializer
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

        private void PostJsonSerializerInitiation(JsonSerializer jsonSerializer)
        {
            if (SaveEnumsAsIntegers == false)
                jsonSerializer.Converters.Add(new StringEnumConverter());

            jsonSerializer.Converters.Add(JsonDateTimeISO8601Converter.Instance);
            jsonSerializer.Converters.Add(JsonLuceneDateTimeConverter.Instance);
            jsonSerializer.Converters.Add(JsonObjectConverter.Instance);
            jsonSerializer.Converters.Add(JsonDictionaryDateTimeKeysConverter.Instance);
            jsonSerializer.Converters.Add(ParametersConverter.Instance);
            jsonSerializer.Converters.Add(JsonLinqEnumerableConverter.Instance);
            jsonSerializer.Converters.Add(JsonIMetadataDictionaryConverter.Instance);
            jsonSerializer.Converters.Add(SizeConverter.Instance);
        }

        /// <summary>
        ///     Creates the serializer.
        /// </summary>
        /// <returns></returns>
        public JsonSerializer CreateSerializer()
        {
            var jsonSerializer = CreateInitialSerializer();
            CustomizeJsonSerializer(jsonSerializer);
            PostJsonSerializerInitiation(jsonSerializer);
            return jsonSerializer;
        }

        /// <summary>
        ///     Creates the serializer.
        /// </summary>
        /// <returns></returns>
        public JsonSerializer CreateDeserializer()
        {
            var jsonSerializer = CreateInitialSerializer();
            CustomizeJsonSerializer(jsonSerializer);
            CustomizeJsonDeserializer(jsonSerializer);
            PostJsonSerializerInitiation(jsonSerializer);
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
        public string GetClrTypeName(object entity)
        {
            if (FindClrTypeNameForDynamic != null && entity is IDynamicMetaObjectProvider)
            {
                try
                {
                    return FindClrTypeNameForDynamic(entity);
                }
                catch (RuntimeBinderException)
                {
                    // if we can't find it, we'll just assume that the property
                    // isn't there
                }
            }

            return FindClrTypeName(entity.GetType());
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

        public RangeType GetRangeType(Type type)
        {
            var nonNullable = Nullable.GetUnderlyingType(type);
            if (nonNullable != null)
                type = nonNullable;

            if (type == typeof(int) || type == typeof(long) || type == typeof(short) || type == typeof(TimeSpan))
                return RangeType.Long;

            if (type == typeof(double) || type == typeof(float) || type == typeof(decimal))
                return RangeType.Double;

            if (_customRangeTypes.TryGetValue(type, out var rangeType))
            {
                return rangeType;
            }

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
#pragma warning disable CS0618 // Type or member is obsolete
                    _prettifyGeneratedLinqExpressions = _originalConfiguration.PrettifyGeneratedLinqExpressions.Value;
#pragma warning restore CS0618 // Type or member is obsolete
                    _readBalanceBehavior = _originalConfiguration.ReadBalanceBehavior.Value;

                    _originalConfiguration = null;
                    return;
                }

                if (_originalConfiguration == null)
                    _originalConfiguration = new ClientConfiguration
                    {
                        Etag = -1,
                        MaxNumberOfRequestsPerSession = MaxNumberOfRequestsPerSession,
#pragma warning disable CS0618 // Type or member is obsolete
                        PrettifyGeneratedLinqExpressions = PrettifyGeneratedLinqExpressions,
#pragma warning restore CS0618 // Type or member is obsolete
                        ReadBalanceBehavior = ReadBalanceBehavior
                    };

                _maxNumberOfRequestsPerSession = configuration.MaxNumberOfRequestsPerSession ?? _originalConfiguration.MaxNumberOfRequestsPerSession.Value;
#pragma warning disable CS0618 // Type or member is obsolete
                _prettifyGeneratedLinqExpressions = configuration.PrettifyGeneratedLinqExpressions ?? _originalConfiguration.PrettifyGeneratedLinqExpressions.Value;
#pragma warning restore CS0618 // Type or member is obsolete
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

        public void RegisterQueryMethodConverter(QueryMethodConverter converter)
        {
            AssertNotFrozen();

            _listOfQueryMethodConverters.Add(converter);
        }

        internal bool AnyQueryMethodConverters => _listOfQueryMethodConverters.Count > 0;

        internal bool TryConvertQueryMethod<T>(QueryMethodConverter.Parameters<T> parameters)
        {
            if (_listOfQueryMethodConverters.Count == 0)
                return false;

            foreach (var converter in _listOfQueryMethodConverters)
            {
                if (converter.Convert(parameters))
                    return true;
            }

            return false;
        }

        public void RegisterQueryValueConverter<T>(TryConvertValueForQueryDelegate<T> converter)
        {
            AssertNotFrozen();

            int index;
            for (index = 0; index < _listOfQueryValueToObjectConverters.Count; index++)
            {
                var entry = _listOfQueryValueToObjectConverters[index];
                if (entry.Type.IsAssignableFrom(typeof(T)))
                    break;
            }

            _listOfQueryValueToObjectConverters.Insert(index, (typeof(T), Actual));

            bool Actual(string name, object value, bool forRange, out object strValue)
            {
                if (value is T t)
                {
                    var result = converter(name, t, forRange, out var str);
                    strValue = str;
                    return result;
                }
                strValue = null;
                return false;
            }
        }

        [Obsolete("Use TryConvertValueForQuery, staying here for backward compact")]
        public bool TryConvertValueForQuery(string fieldName, object value, bool forRange, out string strValue)
        {
            var result = TryConvertValueToObjectForQuery(fieldName, value, forRange, out var output);
            strValue = output as string;
            return result && (strValue != null || output == null);
        }

        private void RegisterQueryValueConverter<T>(TryConvertValueToObjectForQueryDelegate<T> converter)
        {
            AssertNotFrozen();

            int index;
            for (index = 0; index < _listOfQueryValueToObjectConverters.Count; index++)
            {
                var entry = _listOfQueryValueToObjectConverters[index];
                if (entry.Type.IsAssignableFrom(typeof(T)))
                    break;
            }

            _listOfQueryValueToObjectConverters.Insert(index, (typeof(T), Actual));

            bool Actual(string name, object value, bool forRange, out object objValue)
            {
                if (value is T t)
                    return converter(name, t, forRange, out objValue);
                objValue = null;
                return false;
            }
        }

        public void RegisterQueryValueConverter<T>(TryConvertValueToObjectForQueryDelegate<T> converter, RangeType rangeType)
        {
            RegisterQueryValueConverter(converter);

            _customRangeTypes[typeof(T)] = rangeType;
        }

        internal bool TryConvertValueToObjectForQuery(string fieldName, object value, bool forRange, out object objValue)
        {
            foreach (var queryValueConverter in _listOfQueryValueToObjectConverters)
            {
                if (queryValueConverter.Type.IsInstanceOfType(value) == false)
                    continue;

                return queryValueConverter.Convert(fieldName, value, forRange, out objValue);
            }

            objValue = null;
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
        internal void AssertNotFrozen()
        {
            if (_frozen)
                throw new InvalidOperationException(
                    $"Conventions has frozen after '{nameof(DocumentStore)}.{nameof(DocumentStore.Initialize)}()' and no changes can be applied to them.");
        }
    }
}
