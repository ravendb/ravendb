using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Net.Http;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Microsoft.CSharp.RuntimeBinder;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Raven.Client.Util;
using Sparrow;
using Sparrow.Json;
using Sparrow.Platform;
using Sparrow.Utils;
using Size = Sparrow.Size;

namespace Raven.Client.Documents.Conventions
{
    /// <summary>
    ///     The set of conventions used by the <see cref="DocumentStore" /> which allow the users to customize
    ///     the way the Raven client API behaves
    /// </summary>
    /// <inheritdoc cref="DocumentationUrls.Session.Options.Conventions"/>
    public sealed class DocumentConventions : Client.Conventions
    {
        public delegate LinqPathProvider.Result CustomQueryTranslator(LinqPathProvider provider, Expression expression);

        public delegate bool TryConvertValueForQueryDelegate<in T>(string fieldName, T value, bool forRange, out string strValue);

        public delegate bool TryConvertValueToObjectForQueryDelegate<in T>(string fieldName, T value, bool forRange, out object objValue);

#if NETCOREAPP3_1_OR_GREATER
        internal static HttpVersionPolicy? DefaultHttpVersionPolicy;

        internal static TimeSpan? DefaultHttpPooledConnectionIdleTimeout;
#endif

        internal static HttpCompressionAlgorithm DefaultHttpCompressionAlgorithm = HttpCompressionAlgorithm.Gzip;

        internal static readonly DocumentConventions Default = new();

        internal static readonly DocumentConventions DefaultForServer = new()
        {
            SendApplicationIdentifier = false,
            MaxContextSizeToKeep = new Size(PlatformDetails.Is32Bits == false ? 8 : 2, SizeUnit.Megabytes),
#if NETCOREAPP3_1_OR_GREATER
            HttpPooledConnectionLifetime = TimeSpan.FromMinutes(19),
#endif
            DisposeCertificate = false,
            DisableTopologyCache = true
        };

        private static readonly bool DefaultDisableTcpCompression = false;

        private static TypeCache<string> CachedDefaultTypeCollectionNames = new(128);

        private readonly Dictionary<MemberInfo, CustomQueryTranslator> _customQueryTranslators = new();

        private readonly List<(Type Type, TryConvertValueToObjectForQueryDelegate<object> Convert)> _listOfQueryValueToObjectConverters = new();
        private readonly TypeCache<TryConvertValueToObjectForQueryDelegate<object>> _listOfQueryValueToObjectConvertersCache = new(512);

        private readonly List<QueryMethodConverter> _listOfQueryMethodConverters = new();

        private readonly Dictionary<Type, RangeType> _customRangeTypes = new();

        private readonly List<Tuple<Type, Func<string, object, Task<string>>>> _listOfRegisteredIdConventionsAsync = new();

        public readonly BulkInsertConventions BulkInsert;

        public readonly AggressiveCacheConventions AggressiveCache;

        public readonly ShardingConventions Sharding;

        public ISerializationConventions Serialization
        {
            get { return _serialization; }
            set
            {
                AssertNotFrozen();

                _serialization = value ?? throw new ArgumentNullException(nameof(value));
                _serialization.Initialize(this);
            }
        }

        public sealed class AggressiveCacheConventions
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

        public sealed class BulkInsertConventions
        {
            private readonly DocumentConventions _conventions;
            private Func<object, IMetadataDictionary, StreamWriter, bool> _trySerializeEntityToJsonStream;
            private int _timeSeriesBatchSize;

            public Func<object, IMetadataDictionary, StreamWriter, bool> TrySerializeEntityToJsonStream
            {
                get => _trySerializeEntityToJsonStream;
                set
                {
                    _conventions.AssertNotFrozen();
                    _trySerializeEntityToJsonStream = value;
                }
            }

            public int TimeSeriesBatchSize
            {
                get => _timeSeriesBatchSize;
                set
                {
                    _conventions.AssertNotFrozen();

                    if (value <= 0)
                        throw new InvalidOperationException($"{nameof(TimeSeriesBatchSize)} must be positive");

                    _timeSeriesBatchSize = value;
                }
            }

            internal BulkInsertConventions(DocumentConventions conventions)
            {
                _conventions = conventions;
                TrySerializeEntityToJsonStream = null;
                TimeSeriesBatchSize = 1024;
            }
        }

        public sealed class ShardingConventions
        {
            private readonly DocumentConventions _conventions;

            private ShardedBatchBehavior _batchBehavior;

            public ShardedBatchBehavior BatchBehavior
            {
                get => _batchBehavior;
                set
                {
                    _conventions.AssertNotFrozen();

                    _batchBehavior = value;
                }
            }

            internal ShardingConventions(DocumentConventions conventions)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _batchBehavior = ShardedBatchBehavior.Default;
            }
        }

        static DocumentConventions()
        {
#if NETCOREAPP3_1_OR_GREATER
            var httpPooledConnectionLifetimeAsString = Environment.GetEnvironmentVariable("RAVEN_HTTP_POOLEDCONNECTIONLIFETIMEINSEC");
            if (httpPooledConnectionLifetimeAsString != null)
            {
                if (int.TryParse(httpPooledConnectionLifetimeAsString, out var httpPooledConnectionLifetime) == false)
                    throw new InvalidOperationException($"Could not parse 'RAVEN_HTTP_POOLEDCONNECTIONLIFETIMEINSEC' env variable with value '{httpPooledConnectionLifetimeAsString}'.");

                DefaultForServer.HttpPooledConnectionLifetime = httpPooledConnectionLifetime < 0
                    ? null
                    : TimeSpan.FromSeconds(httpPooledConnectionLifetime);
            }

            var httpVersionAsString = Environment.GetEnvironmentVariable("RAVEN_HTTP_VERSION");
            if (httpVersionAsString != null)
            {
                Version httpVersion;
                try
                {
                    httpVersion = Version.Parse(httpVersionAsString);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Could not parse 'RAVEN_HTTP_VERSION' env variable with value '{httpVersionAsString}'.", e);
                }

                DefaultForServer.HttpVersion = httpVersion;
            }

            var httpCompressionAlgorithmAsString = Environment.GetEnvironmentVariable("RAVEN_HTTP_COMPRESSION_ALGORITHM");
            if (httpCompressionAlgorithmAsString != null)
            {
                HttpCompressionAlgorithm httpCompressionAlgorithm;
                try
                {
                    httpCompressionAlgorithm = Enum.Parse<HttpCompressionAlgorithm>(httpCompressionAlgorithmAsString, ignoreCase: true);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Could not parse 'RAVEN_HTTP_COMPRESSION_ALGORITHM' env variable with value '{httpCompressionAlgorithmAsString}'.", e);
                }

                DefaultHttpCompressionAlgorithm = httpCompressionAlgorithm;
                Default.HttpCompressionAlgorithm = httpCompressionAlgorithm;
                DefaultForServer.HttpCompressionAlgorithm = httpCompressionAlgorithm;
            }
#endif

#if NETCOREAPP3_1_OR_GREATER
            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                try
                {
                    ZstdLib.GetMaxCompression(1);
                }
                catch
                {
                    DefaultDisableTcpCompression = true;
                }
            }
#endif

            Default.Freeze();
            DefaultForServer.Freeze();
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="DocumentConventions" /> class.
        /// </summary>
        public DocumentConventions()
        {
            Serialization = new NewtonsoftJsonSerializationConventions();

            _topologyCacheLocation = AppContext.BaseDirectory;

            ReadBalanceBehavior = ReadBalanceBehavior.None;

            FindIdentityProperty = q => q.Name == "Id";
            IdentityPartsSeparator = '/';
            FindIdentityPropertyNameFromCollectionName = _ => "Id";

            FindClrType = (_, doc) =>
            {
                if (doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) &&
                    metadata.TryGet(Constants.Documents.Metadata.RavenClrType, out string clrType))
                    return clrType;

                return null;
            };
            FindClrTypeName = ReflectionUtil.GetFullNameWithoutVersionInformation;
            ResolveTypeFromClrTypeName = Type.GetType;

            TransformTypeCollectionNameToDocumentIdPrefix = DefaultTransformCollectionNameToDocumentIdPrefix;
            FindCollectionName = DefaultGetCollectionName;

            ShouldApplyPropertyNameConverter = DefaultShouldApplyPropertyNameConverter;

            FindPropertyNameForIndex = DefaultFindPropertyNameForIndex;
            FindPropertyNameForDynamicIndex = DefaultFindPropertyNameForDynamicIndex;

            MaxNumberOfRequestsPerSession = 30;

            BulkInsert = new BulkInsertConventions(this);
            Sharding = new ShardingConventions(this);

            PreserveDocumentPropertiesNotFoundOnModel = true;

            var httpCacheSizeInMb = PlatformDetails.Is32Bits ? 32 : 128;
            MaxHttpCacheSize = new Size(httpCacheSizeInMb, SizeUnit.Megabytes);
#if NETSTANDARD2_0
            HttpVersion = System.Net.HttpVersion.Version11;
#else
            HttpVersion = System.Net.HttpVersion.Version20;
#endif

            OperationStatusFetchMode = OperationStatusFetchMode.ChangesApi;

            AddIdFieldToDynamicObjects = true;
            AggressiveCache = new AggressiveCacheConventions(this);

            _firstBroadcastAttemptTimeout = TimeSpan.FromSeconds(5);
            _secondBroadcastAttemptTimeout = TimeSpan.FromSeconds(30);

            _globalHttpClientTimeout = TimeSpan.FromHours(12);

            _waitForIndexesAfterSaveChangesTimeout = TimeSpan.FromSeconds(15);
            _waitForReplicationAfterSaveChangesTimeout = TimeSpan.FromSeconds(15);
            _waitForNonStaleResultsTimeout = TimeSpan.FromSeconds(15);

            _sendApplicationIdentifier = true;
            _maxContextSizeToKeep = PlatformDetails.Is32Bits == false
                ? new Size(1, SizeUnit.Megabytes)
                : new Size(256, SizeUnit.Kilobytes);

            _disableTcpCompression = DefaultDisableTcpCompression;

            _httpClientType = typeof(HttpClient);
            _createHttpClient = handler => new HttpClient(handler);

            _disposeCertificate = true;

            _httpCompressionAlgorithm = DefaultHttpCompressionAlgorithm;
        }

        private bool _frozen;
        private ClientConfiguration _originalConfiguration;

        private static readonly TypeCache<MemberInfo> IdPropertyCache = new(1024);

        private bool _saveEnumsAsIntegers;
        private bool _saveEnumsAsIntegersForPatching;
        private char _identityPartsSeparator;
        private bool _disableTopologyUpdates;

        private Func<InMemoryDocumentSessionOperations, object, string, bool> _shouldIgnoreEntityChanges;
        private Func<MemberInfo, bool> _findIdentityProperty;
        private Func<string, string> _transformTypeCollectionNameToDocumentIdPrefix;
        private Func<string, object, Task<string>> _asyncDocumentIdGenerator;
        private Func<string, string> _findIdentityPropertyNameFromCollectionName;
        private Func<Type, string, string, string, string> _findPropertyNameForDynamicIndex;
        private Func<Type, string, string, string, string> _findPropertyNameForIndex;
        private Func<Type, string, string, string, string> _findProjectedPropertyNameForIndex;
        private Func<string, string> _loadBalancerPerSessionContextSelector;

        private Func<dynamic, string> _findCollectionNameForDynamic;
        private Func<dynamic, string> _findClrTypeNameForDynamic;
        private Func<Type, string> _findCollectionName;

        private Func<Type, string> _findClrTypeName;
        private Func<string, BlittableJsonReaderObject, string> _findClrType;
        private Func<string, Type> _resolveTypeFromClrTypeName;
        private bool _useOptimisticConcurrency;
        private bool _addIdFieldToDynamicObjects;
        private int _maxNumberOfRequestsPerSession;

        private TimeSpan? _requestTimeout;
        private TimeSpan _secondBroadcastAttemptTimeout;
        private TimeSpan _firstBroadcastAttemptTimeout;
        private TimeSpan _waitForIndexesAfterSaveChangesTimeout;
        private TimeSpan _waitForReplicationAfterSaveChangesTimeout;
        private TimeSpan _waitForNonStaleResultsTimeout;
        private TimeSpan _globalHttpClientTimeout;

        private int _loadBalancerContextSeed;
        private LoadBalanceBehavior _loadBalanceBehavior;
        private ReadBalanceBehavior _readBalanceBehavior;
        private bool _preserveDocumentPropertiesNotFoundOnModel;
        private Size _maxHttpCacheSize;
        private bool? _useHttpDecompression;
        private bool? _useHttpCompression;
        private HttpCompressionAlgorithm _httpCompressionAlgorithm;
        private Func<MemberInfo, string> _propertyNameConverter;
        private Func<Type, bool> _typeIsKnownServerSide = _ => false;
        private Func<MemberInfo, bool> _shouldApplyPropertyNameConverter;
        private OperationStatusFetchMode _operationStatusFetchMode;
        private bool _disableTopologyCache;
        private string _topologyCacheLocation;
        private Version _httpVersion;
#if NETCOREAPP3_1_OR_GREATER
        private HttpVersionPolicy? _httpVersionPolicy;
#endif
        private Type _httpClientType;
        private Func<HttpClientHandler, HttpClient> _createHttpClient;
#if NETCOREAPP3_1_OR_GREATER
        private TimeSpan? _httpPooledConnectionLifetime;
        private TimeSpan? _httpPooledConnectionIdleTimeout;
#endif
        private bool _sendApplicationIdentifier;
        private Size _maxContextSizeToKeep;
        private ISerializationConventions _serialization;
        private bool? _disableAtomicDocumentWritesInClusterWideTransaction;
        private bool _disableTcpCompression;
        private bool _disposeCertificate;

        public Func<InMemoryDocumentSessionOperations, object, string, bool> ShouldIgnoreEntityChanges
        {
            get => _shouldIgnoreEntityChanges;
            set
            {
                AssertNotFrozen();
                _shouldIgnoreEntityChanges = value;
            }
        }

        public Func<MemberInfo, bool> ShouldApplyPropertyNameConverter
        {
            get => _shouldApplyPropertyNameConverter;
            set
            {
                AssertNotFrozen();
                _shouldApplyPropertyNameConverter = value;
            }
        }

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

#if NETCOREAPP3_1_OR_GREATER
        public HttpVersionPolicy? HttpVersionPolicy
        {
            get => _httpVersionPolicy ?? DefaultHttpVersionPolicy;
            set
            {
                AssertNotFrozen();
                _httpVersionPolicy = value;
            }
        }
#endif

        /// <summary>
        /// Used by HttpClient cache, if you are overriding DocumentConventions.CreateHttpClient convention then it is advisable to return here the type of the HttpClient from that convention
        /// </summary>
        public Type HttpClientType
        {
            get => _httpClientType;
            set
            {
                AssertNotFrozen();
                _httpClientType = value;
            }
        }

        public Func<HttpClientHandler, HttpClient> CreateHttpClient
        {
            get => _createHttpClient;
            set
            {
                AssertNotFrozen();
                _createHttpClient = value;
            }
        }

#if NETCOREAPP3_1_OR_GREATER
        public TimeSpan? HttpPooledConnectionLifetime
        {
            get => _httpPooledConnectionLifetime;
            set
            {
                AssertNotFrozen();
                _httpPooledConnectionLifetime = value;
            }
        }

        public TimeSpan? HttpPooledConnectionIdleTimeout
        {
            get => _httpPooledConnectionIdleTimeout ?? DefaultHttpPooledConnectionIdleTimeout;
            set
            {
                AssertNotFrozen();
                _httpPooledConnectionIdleTimeout = value;
            }
        }
#endif

        internal string GetConvertedPropertyNameFor(MemberInfo member)
        {
            if (member is null)
                return null;

            var converter = PropertyNameConverter;
            if (converter == null)
                return member?.Name;

            if (ShouldApplyPropertyNameConverter(member) == false)
                return member.Name;

            return converter(member);
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

        public TimeSpan GlobalHttpClientTimeout
        {
            get => _globalHttpClientTimeout;
            set
            {
                AssertNotFrozen();
                _globalHttpClientTimeout = value;
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

        public HttpCompressionAlgorithm HttpCompressionAlgorithm
        {
            get => _httpCompressionAlgorithm;
            set
            {
                AssertNotFrozen();
                _httpCompressionAlgorithm = value;
            }
        }

        /// <summary>
        /// Use gzip compression when sending content of HTTP request
        /// </summary>
        public bool UseHttpCompression
        {
            get => _useHttpCompression ?? true;
            set
            {
                AssertNotFrozen();
                _useHttpCompression = value;
            }
        }

        internal bool HasExplicitlySetDecompressionUsage => _useHttpDecompression.HasValue;

        /// <summary>
        /// Can accept compressed HTTP response content and will use zstd/gzip/deflate decompression methods
        /// </summary>
        public bool UseHttpDecompression
        {
            get => _useHttpDecompression ?? true;
            set
            {
                AssertNotFrozen();
                _useHttpDecompression = value;
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

        public ReadBalanceBehavior ReadBalanceBehavior
        {
            get => _readBalanceBehavior;
            set
            {
                AssertNotFrozen();
                _readBalanceBehavior = value;
            }
        }

        public int LoadBalancerContextSeed
        {
            get => _loadBalancerContextSeed;
            set
            {
                AssertNotFrozen();
                _loadBalancerContextSeed = value;
            }
        }

        public LoadBalanceBehavior LoadBalanceBehavior
        {
            get => _loadBalanceBehavior;
            set
            {
                AssertNotFrozen();
                _loadBalanceBehavior = value;
            }
        }

        /// <summary>
        /// Gets or set the function that allow to specialize the topology
        /// selection for a particular session. Used in load balancing
        /// scenarios.
        /// </summary>
        public Func<string, string> LoadBalancerPerSessionContextSelector
        {
            get => _loadBalancerPerSessionContextSelector;
            set
            {
                AssertNotFrozen();
                _loadBalancerPerSessionContextSelector = value;
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
        ///     Gets or sets the function to resolve the clr type from a clr type name.
        /// </summary>
        public Func<string, Type> ResolveTypeFromClrTypeName
        {
            get => _resolveTypeFromClrTypeName;
            set
            {
                AssertNotFrozen();
                _resolveTypeFromClrTypeName = value;
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
        public char IdentityPartsSeparator
        {
            get => _identityPartsSeparator;
            set
            {
                AssertNotFrozen();

                if (value == '|')
                    throw new InvalidOperationException("Cannot set identity parts separator to '|'.");

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
        ///     Patches Enums as integers.
        /// </summary>
        public bool SaveEnumsAsIntegersForPatching
        {
            get => _saveEnumsAsIntegersForPatching;
            set
            {
                AssertNotFrozen();
                _saveEnumsAsIntegersForPatching = value;
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
        /// Disables the usage of topology cache.
        /// </summary>
        public bool DisableTopologyCache
        {
            get => _disableTopologyCache;
            set
            {
                AssertNotFrozen();

                _disableTopologyCache = value;
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

        /// <summary>
        /// Disposes the 'DocumentStore.Certificate' during DocumentStore disposal
        /// </summary>
        public bool DisposeCertificate
        {
            get => _disposeCertificate;
            set
            {
                AssertNotFrozen();

                _disposeCertificate = value;
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
            if (CachedDefaultTypeCollectionNames.TryGet(t, out var result))
                return result;

            if (t.Name.Contains("<>"))
            {
                CachedDefaultTypeCollectionNames.Put(t, null);
                return null;
            }

            // we want to reject queries and other operations on abstract types, because you usually
            // want to use them for polymorphic queries, and that require the conventions to be
            // applied properly, so we reject the behavior and hint to the user explicitly
            if (t.IsInterface)
                throw new InvalidOperationException("Cannot find collection name for interface " + t.FullName +
                                                    ", only concrete classes are supported. Did you forget to customize Conventions.FindCollectionName?");
            if (t.IsAbstract)
                throw new InvalidOperationException("Cannot find collection name for abstract class " + t.FullName +
                                                    ", only concrete class are supported. Did you forget to customize Conventions.FindCollectionName?");

            if (t.IsGenericType)
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
                result = null;
            }
            else
            {
                result = Inflector.Pluralize(t.Name);
            }

            CachedDefaultTypeCollectionNames.Put(t, result);
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
            // If we had obtained the identity property for that type, we will return it.
            var currentIdPropertyCache = IdPropertyCache;
            if (currentIdPropertyCache.TryGet(type, out var identityProperty))
                return identityProperty;

            identityProperty = GetPropertiesForType(type).FirstOrDefault(FindIdentityProperty);
            if (identityProperty != null && identityProperty.DeclaringType != type)
            {
                var propertyInfo = identityProperty.DeclaringType.GetProperty(identityProperty.Name);
                identityProperty = propertyInfo ?? identityProperty;
            }

            // Add it to the cache.
            currentIdPropertyCache.Put(type, identityProperty);

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
                    _maxNumberOfRequestsPerSession = _originalConfiguration.MaxNumberOfRequestsPerSession ?? _maxNumberOfRequestsPerSession;
                    _readBalanceBehavior = _originalConfiguration.ReadBalanceBehavior ?? _readBalanceBehavior;
                    _identityPartsSeparator = _originalConfiguration.IdentityPartsSeparator ?? _identityPartsSeparator;
                    _loadBalanceBehavior = _originalConfiguration.LoadBalanceBehavior ?? _loadBalanceBehavior;
                    _loadBalancerContextSeed = _originalConfiguration.LoadBalancerContextSeed ?? _loadBalancerContextSeed;

                    _originalConfiguration = null;
                    return;
                }

                _originalConfiguration ??= new ClientConfiguration
                {
                    Etag = -1,
                    MaxNumberOfRequestsPerSession = MaxNumberOfRequestsPerSession,
                    ReadBalanceBehavior = ReadBalanceBehavior,
                    IdentityPartsSeparator = IdentityPartsSeparator,
                    LoadBalanceBehavior = _loadBalanceBehavior,
                    LoadBalancerContextSeed = _loadBalancerContextSeed
                };

                _maxNumberOfRequestsPerSession = configuration.MaxNumberOfRequestsPerSession ?? _originalConfiguration.MaxNumberOfRequestsPerSession ?? _maxNumberOfRequestsPerSession;
                _readBalanceBehavior = configuration.ReadBalanceBehavior ?? _originalConfiguration.ReadBalanceBehavior ?? _readBalanceBehavior;
                _loadBalanceBehavior = configuration.LoadBalanceBehavior ?? _originalConfiguration.LoadBalanceBehavior ?? _loadBalanceBehavior;
                _loadBalancerContextSeed = configuration.LoadBalancerContextSeed ?? _originalConfiguration.LoadBalancerContextSeed ?? _loadBalancerContextSeed;
                _identityPartsSeparator = configuration.IdentityPartsSeparator ?? _originalConfiguration.IdentityPartsSeparator ?? _identityPartsSeparator;
            }
        }

        public static bool DefaultShouldApplyPropertyNameConverter(MemberInfo member) => (member.DeclaringType?.Namespace?.StartsWith("System") == true ||
                                                                                            member.DeclaringType?.Namespace?.StartsWith("Microsoft") == true) == false;
        public static string DefaultTransformCollectionNameToDocumentIdPrefix(string collectionName)
        {
            var count = collectionName.Count(char.IsUpper);

            if (count <= 1) // simple name, just lower case it
                return collectionName.ToLowerInvariant();

            // multiple capital letters, so probably something that we want to preserve caps on.
            return collectionName;
        }

        public static string DefaultFindPropertyNameForIndex(Type indexedType, string indexedName, string path, string prop) => (path + prop).Replace("[].", "_").Replace(".", "_");

        public static string DefaultFindPropertyNameForDynamicIndex(Type indexedType, string indexedName, string path, string prop) => path + prop;

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

        /// <summary>
        /// EXPERT: Disable automatic atomic writes with cluster write transactions. If set to 'true', will only consider explicitly
        /// added compare exchange values to validate cluster wide transactions."
        /// </summary>
        public bool? DisableAtomicDocumentWritesInClusterWideTransaction
        {
            get => _disableAtomicDocumentWritesInClusterWideTransaction;
            set
            {
                AssertNotFrozen();
                _disableAtomicDocumentWritesInClusterWideTransaction = value;
            }
        }

        /// <summary>
        /// Disables the usage of TCP data compression.
        /// </summary>
        public bool DisableTcpCompression
        {
            get => _disableTcpCompression;
            set
            {
                AssertNotFrozen();

                _disableTcpCompression = value;
            }
        }

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

        internal bool TryConvertValueToObjectForQuery(string fieldName, Type baseType, object value, bool forRange, out object objValue)
        {
            // We try to find this converter in the cache.
            if (!_listOfQueryValueToObjectConvertersCache.TryGet(baseType, out var objValueFunc))
            {
                // Because we haven't find it, we need to do the actual work of trying out every single converter
                // in order to find the one that matches for this object type.
                foreach (var queryValueConverter in _listOfQueryValueToObjectConverters)
                {
                    if (queryValueConverter.Type.IsInstanceOfType(value) == false)
                        continue;

                    objValueFunc = queryValueConverter.Convert;

                    // Store in the cache.
                    _listOfQueryValueToObjectConvertersCache.Put(baseType, objValueFunc);
                    break;
                }
            }

            if (objValueFunc != null)
                return objValueFunc(fieldName, value, forRange, out objValue);

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

        internal TestingStuff ForTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (ForTestingPurposes != null)
                return ForTestingPurposes;

            return ForTestingPurposes = new TestingStuff();
        }

        internal class TestingStuff
        {
            internal Action<RequestExecutor> OnBeforeTopologyUpdate;
        }
    }
}
