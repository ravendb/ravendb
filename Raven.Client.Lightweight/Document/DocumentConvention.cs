//-----------------------------------------------------------------------
// <copyright file="DocumentConvention.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization.Formatters;
using System.Text;
using System.Threading.Tasks;
using Microsoft.CSharp.RuntimeBinder;

using Raven.Abstractions.Cluster;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Replication;
using Raven.Client.Connection.Async;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Serialization;
using Raven.Abstractions;
using Raven.Abstractions.Json;
using Raven.Client.Connection;
using Raven.Client.Converters;
using Raven.Client.Util;
using Raven.Json.Linq;
using Raven.Abstractions.Extensions;
using Raven.Client.Metrics;

namespace Raven.Client.Document
{
    /// <summary>
    /// The set of conventions used by the <see cref="DocumentStore"/> which allow the users to customize
    /// the way the Raven client API behaves
    /// </summary>
    public class DocumentConvention : QueryConvention
    {
        public delegate IEnumerable<object> ApplyReduceFunctionFunc(
            Type indexType,
            Type resultType,
            IEnumerable<object> results,
            Func<Func<IEnumerable<object>, IEnumerable>> generateTransformResults);

        private Dictionary<Type, Func<IEnumerable<object>, IEnumerable>> compiledReduceCache = new Dictionary<Type, Func<IEnumerable<object>, IEnumerable>>();

        private readonly IList<Tuple<Type, Func<string, IDatabaseCommands, object, string>>> listOfRegisteredIdConventions =
            new List<Tuple<Type, Func<string, IDatabaseCommands, object, string>>>();

        private readonly IList<Tuple<Type, Func<string, IAsyncDatabaseCommands, object, Task<string>>>> listOfRegisteredIdConventionsAsync =
            new List<Tuple<Type, Func<string, IAsyncDatabaseCommands, object, Task<string>>>>();

        private readonly IList<Tuple<Type, Func<ValueType, string>>> listOfRegisteredIdLoadConventions = 
            new List<Tuple<Type, Func<ValueType, string>>>();

        /// <summary>
        /// Initializes a new instance of the <see cref="DocumentConvention"/> class.
        /// </summary>
        public DocumentConvention()
        {
            IdentityTypeConvertors = new List<ITypeConverter>
            {
                new GuidConverter(),
                new Int32Converter(),
                new Int64Converter(),
            };
            PreserveDocumentPropertiesNotFoundOnModel = true;
#if !DNXCORE50
            PrettifyGeneratedLinqExpressions = true;
#endif
            DisableProfiling = true;
            EnlistInDistributedTransactions = true;
            UseParallelMultiGet = true;
            DefaultQueryingConsistency = ConsistencyOptions.None;
            FailoverBehavior = FailoverBehavior.AllowReadsFromSecondaries;
            ShouldCacheRequest = url => true;
            FindIdentityProperty = q => q.Name == "Id";
            FindClrType = (id, doc, metadata) => metadata.Value<string>(Abstractions.Data.Constants.RavenClrType);

            FindClrTypeName = ReflectionUtil.GetFullNameWithoutVersionInformation;
            TransformTypeTagNameToDocumentKeyPrefix = DefaultTransformTypeTagNameToDocumentKeyPrefix;
            FindFullDocumentKeyFromNonStringIdentifier = DefaultFindFullDocumentKeyFromNonStringIdentifier;
            FindIdentityPropertyNameFromEntityName = entityName => "Id";
            FindTypeTagName = DefaultTypeTagName;
            FindPropertyNameForIndex = (indexedType, indexedName, path, prop) => (path + prop).Replace(",", "_").Replace(".", "_");
            FindPropertyNameForDynamicIndex = (indexedType, indexedName, path, prop) => path + prop;
            IdentityPartsSeparator = "/";
            JsonContractResolver = new DefaultRavenContractResolver(shareCache: true)
            {
                DefaultMembersSearchFlags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance
            };
            MaxNumberOfRequestsPerSession = 30;
            MaxLengthOfQueryUsingGetUrl = 1024 + 512;
            ApplyReduceFunction = DefaultApplyReduceFunction;
            ReplicationInformerFactory = (url, jsonRequestFactory, requestTimeMetricGetter) => new ReplicationInformer(this, jsonRequestFactory, requestTimeMetricGetter);
            CustomizeJsonSerializer = serializer => { };
            FindIdValuePartForValueTypeConversion = (entity, id) => id.Split(new[] { IdentityPartsSeparator }, StringSplitOptions.RemoveEmptyEntries).Last();
            ShouldAggressiveCacheTrackChanges = true;
            ShouldSaveChangesForceAggressiveCacheCheck = true;
            IndexAndTransformerReplicationMode = IndexAndTransformerReplicationMode.Indexes | IndexAndTransformerReplicationMode.Transformers;
            AcceptGzipContent = true;
            RequestTimeSlaThresholdInMilliseconds = 100;
        }

        private IEnumerable<object> DefaultApplyReduceFunction(
            Type indexType,
            Type resultType,
            IEnumerable<object> results,
            Func<Func<IEnumerable<object>, IEnumerable>> generateTransformResults)
        {
            var copy = compiledReduceCache;
            Func<IEnumerable<object>, IEnumerable> compile;
            if (copy.TryGetValue(indexType, out compile) == false)
            {
                compile = generateTransformResults();
                compiledReduceCache = new Dictionary<Type, Func<IEnumerable<object>, IEnumerable>>(copy)
                {
                    {indexType, compile}
                };
            }
            return compile(results).Cast<object>()
                .Select(result =>
                {
                    // we got an anonymous object and we need to get the reduce results
                    var ravenJTokenWriter = new RavenJTokenWriter();
                    var jsonSerializer = CreateSerializer();
                    jsonSerializer.Serialize(ravenJTokenWriter, result);
                    return jsonSerializer.Deserialize(new RavenJTokenReader(ravenJTokenWriter.Token), resultType);
                });
        }

        public static string DefaultTransformTypeTagNameToDocumentKeyPrefix(string typeTagName)
        {
            var count = typeTagName.Count(char.IsUpper);

            if (count <= 1) // simple name, just lower case it
                return typeTagName.ToLowerInvariant();

            // multiple capital letters, so probably something that we want to preserve caps on.
            return typeTagName;
        }

        ///<summary>
        /// Find the full document name assuming that we are using the standard conventions
        /// for generating a document key
        ///</summary>
        ///<returns></returns>
        public string DefaultFindFullDocumentKeyFromNonStringIdentifier(object id, Type type, bool allowNull)
        {
            var valueTypeId = id as ValueType;
            if (valueTypeId != null)
            {
                var outputId = TryGetDocumentIdFromRegisteredIdLoadConventions(valueTypeId, type);
                if (outputId != null)
                    return outputId;
            }
            

            var converter = IdentityTypeConvertors.FirstOrDefault(x => x.CanConvertFrom(id.GetType()));
            var tag = GetTypeTagName(type);
            if (tag != null)
            {
                tag = TransformTypeTagNameToDocumentKeyPrefix(tag);
                tag += IdentityPartsSeparator;
            }
            if (converter != null)
            {
                return converter.ConvertFrom(tag, id, allowNull);
            }
            return tag + id;
        }

        /// <summary>
        /// Tries to get the full document id/key from a "simple" id to the full id.
        /// </summary>
        /// <param name="id">Simple id.</param>
        /// <returns>The full document id, null if no registered id load conventions found</returns>
        public string TryGetDocumentIdFromRegisteredIdLoadConventions<TEntity>(ValueType id)
        {
            return TryGetDocumentIdFromRegisteredIdLoadConventions(id, typeof(TEntity));
        }

        /// <summary>
        /// Tries to get the full document id/key from a "simple" id to the full id.
        /// </summary>
        /// <param name="id">Simple id.</param>
        /// <param name="type">Document type</param>
        /// <returns>Full document id, null if no registered id load conventions found</returns>
        public string TryGetDocumentIdFromRegisteredIdLoadConventions(ValueType id, Type type)
        {
            foreach (var typeToRegisteredIdLoadConvention in listOfRegisteredIdLoadConventions
                .Where(typeToRegisteredIdConvention => typeToRegisteredIdConvention.Item1.IsAssignableFrom(type)))
            {
                return typeToRegisteredIdLoadConvention.Item2(id);
            }

            return null;
        }

        /// <summary>
        /// Register an action to customize the json serializer used by the <see cref="DocumentStore"/>
        /// </summary>
        public Action<JsonSerializer> CustomizeJsonSerializer { get; set; }

        /// <summary>
        /// Disable all profiling support
        /// </summary>
        public bool DisableProfiling { get; set; }

        ///<summary>
        /// A list of type converters that can be used to translate the document key (string)
        /// to whatever type it is that is used on the entity, if the type isn't already a string
        ///</summary>
        public List<ITypeConverter> IdentityTypeConvertors { get; set; }

        /// <summary>
        /// Gets or sets the max length of Url of GET requests.
        /// </summary>
        /// <value>The max number of requests per session.</value>
        public int MaxNumberOfRequestsPerSession { get; set; }

        /// <summary>
        /// Gets or sets the default max length of a query using the GET method against a server.
        /// </summary>
        public int MaxLengthOfQueryUsingGetUrl { get; set; }

        /// <summary>
        /// Whether to allow queries on document id.
        /// By default, queries on id are disabled, because it is far more efficient
        /// to do a Load() than a Query() if you already know the id.
        /// This is NOT recommended and provided for backward compatibility purposes only.
        /// </summary>
        public bool AllowQueriesOnId { get; set; }

        /// <summary>
        /// The consistency options used when querying the database by default
        /// Note that this option impact only queries, since we have Strong Consistency model for the documents
        /// </summary>
        public ConsistencyOptions DefaultQueryingConsistency { get; set; }


        /// <summary>
        /// Whether UseOptimisticConcurrency is set to true by default for all opened sessions
        /// </summary>
        public bool DefaultUseOptimisticConcurrency { get; set; }

        /// <summary>
        /// Generates the document key using identity.
        /// </summary>
        /// <param name="conventions">The conventions.</param>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        public static string GenerateDocumentKeyUsingIdentity(DocumentConvention conventions, object entity)
        {
            return conventions.GetDynamicTagName(entity) + "/";
        }

        private static IDictionary<Type, string> cachedDefaultTypeTagNames = new Dictionary<Type, string>();

        /// <summary>
        /// Get the default tag name for the specified type.
        /// </summary>
        public static string DefaultTypeTagName(Type t)
        {
            string result;
            if (cachedDefaultTypeTagNames.TryGetValue(t, out result))
                return result;

            if (t.Name.Contains("<>"))
                return null;
            if (t.IsGenericType())
            {
                var name = t.GetGenericTypeDefinition().Name;
                if (name.Contains('`'))
                {
                    name = name.Substring(0, name.IndexOf('`'));
                }
                var sb = new StringBuilder(Inflector.Pluralize(name));
                foreach (var argument in t.GetGenericArguments())
                {
                    sb.Append("Of")
                        .Append(DefaultTypeTagName(argument));
                }
                result = sb.ToString();
            }
            else
            {
                result = Inflector.Pluralize(t.Name);
            }
            var temp = new Dictionary<Type, string>(cachedDefaultTypeTagNames);
            temp[t] = result;
            cachedDefaultTypeTagNames = temp;
            return result;
        }

        /// <summary>
        /// Gets the name of the type tag.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        public string GetTypeTagName(Type type)
        {
            return FindTypeTagName(type) ?? DefaultTypeTagName(type);
        }

       /// <summary>
       /// If object is dynamic, try to load a tag name.
       /// </summary>
       /// <param name="entity">Current entity.</param>
       /// <returns>Dynamic tag name if available.</returns>
       public string GetDynamicTagName(object entity)
       {
          if (entity == null)
          {
             return null;
          }

          if (FindDynamicTagName != null && entity is IDynamicMetaObjectProvider)
          {
             try
             {
                return FindDynamicTagName(entity);
             }
             catch (RuntimeBinderException)
             {
             }
          }

          return GetTypeTagName(entity.GetType());
       }

        /// <summary>
        /// Generates the document key.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <param name="dbName">Name of the database</param>
        /// <param name="databaseCommands">Low level database commands.</param>
        /// <returns></returns>
        public string GenerateDocumentKey(string dbName, IDatabaseCommands databaseCommands, object entity)
        {
            var type = entity.GetType();
            foreach (var typeToRegisteredIdConvention in listOfRegisteredIdConventions
                .Where(typeToRegisteredIdConvention => typeToRegisteredIdConvention.Item1.IsAssignableFrom(type)))
            {
                return typeToRegisteredIdConvention.Item2(dbName, databaseCommands, entity);
            }

            if (listOfRegisteredIdConventionsAsync.Any(x => x.Item1.IsAssignableFrom(type)))
            {
                throw new InvalidOperationException("Id convention for synchronous operation was not found for entity " + type.FullName + ", but convention for asynchronous operation exists.");
            }

            return DocumentKeyGenerator(dbName, databaseCommands, entity);
        }

        public Task<string> GenerateDocumentKeyAsync(string dbName, IAsyncDatabaseCommands databaseCommands, object entity)
        {
            var type = entity.GetType();
            foreach (var typeToRegisteredIdConvention in listOfRegisteredIdConventionsAsync
                .Where(typeToRegisteredIdConvention => typeToRegisteredIdConvention.Item1.IsAssignableFrom(type)))
            {
                return typeToRegisteredIdConvention.Item2(dbName, databaseCommands, entity);
            }

            if (listOfRegisteredIdConventions.Any(x => x.Item1.IsAssignableFrom(type)))
            {
                throw new InvalidOperationException("Id convention for asynchronous operation was not found for entity " + type.FullName + ", but convention for synchronous operation exists.");
            }

            return AsyncDocumentKeyGenerator(dbName, databaseCommands, entity);
        }

        /// <summary>
        /// Gets or sets the function to find the clr type of a document.
        /// </summary>
        public Func<string, RavenJObject, RavenJObject, string> FindClrType { get; set; }

        /// <summary>
        /// Gets or sets the function to find the clr type name from a clr type
        /// </summary>
        public Func<Type, string> FindClrTypeName { get; set; }

        /// <summary>
        /// Gets or sets the function to find the full document key based on the type of a document
        /// and the value type identifier (just the numeric part of the id).
        /// </summary>
        public Func<object, Type, bool, string> FindFullDocumentKeyFromNonStringIdentifier { get; set; }

        /// <summary>
        /// Gets or sets the json contract resolver.
        /// </summary>
        /// <value>The json contract resolver.</value>
        public IContractResolver JsonContractResolver { get; set; }

        /// <summary>
        /// Gets or sets the function to find the type tag.
        /// </summary>
        /// <value>The name of the find type tag.</value>
        public Func<Type, string> FindTypeTagName { get; set; }

      /// <summary>
      /// Gets or sets the function to find the tag name if the object is dynamic.
      /// </summary>
      /// <value>The tag name.</value>
      public Func<dynamic, string> FindDynamicTagName { get; set; }

        /// <summary>
        /// Gets or sets the function to find the indexed property name
        /// given the indexed document type, the index name, the current path and the property path.
        /// </summary>
        public Func<Type, string, string, string, string> FindPropertyNameForIndex { get; set; }

        /// <summary>
        /// Gets or sets the function to find the indexed property name
        /// given the indexed document type, the index name, the current path and the property path.
        /// </summary>
        public Func<Type, string, string, string, string> FindPropertyNameForDynamicIndex { get; set; }

        /// <summary>
        /// Get or sets the function to get the identity property name from the entity name
        /// </summary>
        public Func<string, string> FindIdentityPropertyNameFromEntityName { get; set; }

        /// <summary>
        /// Gets or sets the document key generator.
        /// </summary>
        /// <value>The document key generator.</value>
        public Func<string, IDatabaseCommands, object, string> DocumentKeyGenerator { get; set; }

        /// <summary>
        /// Gets or sets the document key generator.
        /// </summary>
        /// <value>The document key generator.</value>
        public Func<string, IAsyncDatabaseCommands, object, Task<string>> AsyncDocumentKeyGenerator { get; set; }

        /// <summary>
        /// Instruct RavenDB to parallel Multi Get processing 
        /// when handling lazy requests
        /// </summary>
        public bool UseParallelMultiGet { get; set; }

        /// <summary>
        /// Whether or not RavenDB should in the aggressive cache mode use Changes API to track
        /// changes and rebuild the cache. This will make that outdated data will be revalidated
        /// to make the cache more updated, however it is still possible to get a state result because of the time
        /// needed to receive the notification and forcing to check for cached data.
        /// </summary>
        public bool ShouldAggressiveCacheTrackChanges { get; set; }

        /// <summary>
        /// Whether or not RavenDB should in the aggressive cache mode should force the aggressive cache
        /// to check with the server after we called SaveChanges() on a non empty data set.
        /// This will make any outdated data revalidated, and will work nicely as long as you have just a 
        /// single client. For multiple clients, <see cref="ShouldAggressiveCacheTrackChanges"/>.
        /// </summary>
        public bool ShouldSaveChangesForceAggressiveCacheCheck { get; set; }

        /// <summary>
        /// Register an id convention for a single type (and all of its derived types.
        /// Note that you can still fall back to the DocumentKeyGenerator if you want.
        /// </summary>
        public DocumentConvention RegisterIdConvention<TEntity>(Func<string, IDatabaseCommands, TEntity, string> func)
        {
            var type = typeof(TEntity);
            var entryToRemove = listOfRegisteredIdConventions.FirstOrDefault(x => x.Item1 == type);
            if (entryToRemove != null)
            {
                listOfRegisteredIdConventions.Remove(entryToRemove);
            }

            int index;
            for (index = 0; index < listOfRegisteredIdConventions.Count; index++)
            {
                var entry = listOfRegisteredIdConventions[index];
                if (entry.Item1.IsAssignableFrom(type))
                {
                    break;
                }
            }

            var item = new Tuple<Type, Func<string, IDatabaseCommands, object, string>>(typeof(TEntity), (dbName, commands, o) => func(dbName, commands, (TEntity)o));
            listOfRegisteredIdConventions.Insert(index, item);

            return this;
        }

        /// <summary>
        /// Register an async id convention for a single type (and all of its derived types.
        /// Note that you can still fall back to the DocumentKeyGenerator if you want.
        /// </summary>
        public DocumentConvention RegisterAsyncIdConvention<TEntity>(Func<string, IAsyncDatabaseCommands, TEntity, Task<string>> func)
        {
            var type = typeof(TEntity);
            var entryToRemove = listOfRegisteredIdConventionsAsync.FirstOrDefault(x => x.Item1 == type);
            if (entryToRemove != null)
            {
                listOfRegisteredIdConventionsAsync.Remove(entryToRemove);
            }

            int index;
            for (index = 0; index < listOfRegisteredIdConventionsAsync.Count; index++)
            {
                var entry = listOfRegisteredIdConventionsAsync[index];
                if (entry.Item1.IsAssignableFrom(type))
                {
                    break;
                }
            }

            var item = new Tuple<Type, Func<string, IAsyncDatabaseCommands, object, Task<string>>>(typeof(TEntity), (dbName, commands, o) => func(dbName, commands, (TEntity)o));
            listOfRegisteredIdConventionsAsync.Insert(index, item);

            return this;
        }

        /// <summary>
        /// Register an id convention for a single type (and all its derived types) to be used when calling session.Load{TEntity}(TId id)
        /// It is used by the default implementation of FindFullDocumentKeyFromNonStringIdentifier.
        /// </summary>
        public DocumentConvention RegisterIdLoadConvention<TEntity>(Func<ValueType, string> func)
        {
            var type = typeof(TEntity);
            var entryToRemove = listOfRegisteredIdLoadConventions.FirstOrDefault(x => x.Item1 == type);
            if (entryToRemove != null)
                listOfRegisteredIdLoadConventions.Remove(entryToRemove);

            int index;
            for (index = 0; index < listOfRegisteredIdLoadConventions.Count; index++)
            {
                var entry = listOfRegisteredIdLoadConventions[index];
                if (entry.Item1.IsAssignableFrom(type))
                    break;
            }

            var item = new Tuple<Type, Func<ValueType, string>>(typeof(TEntity), o => func(o));
            listOfRegisteredIdLoadConventions.Insert(index, item);

            return this;
        }


        private static Lazy<JsonConverterCollection> defaultConverters = new Lazy<JsonConverterCollection>(() =>
        {
            var converters = new JsonConverterCollection(Default.Converters);
            converters.Add(new JsonLuceneDateTimeConverter());
            converters.Add(new JsonNumericConverter<int>(int.TryParse));
            converters.Add(new JsonNumericConverter<long>(long.TryParse));
            converters.Add(new JsonNumericConverter<decimal>(decimal.TryParse));
            converters.Add(new JsonNumericConverter<double>(double.TryParse));
            converters.Add(new JsonNumericConverter<short>(short.TryParse));
            converters.Add(new JsonMultiDimensionalArrayConverter());
            converters.Add(new JsonDynamicConverter());
            converters.Add(new JsonLinqEnumerableConverter());
            converters.Freeze();

            return converters;
        }, true);

        private static Lazy<JsonConverterCollection> defaultConvertersEnumsAsIntegers = new Lazy<JsonConverterCollection>(() =>
        {
            var converters = new JsonConverterCollection(DefaultConverters);

            var converter = converters.FirstOrDefault(x => x is JsonEnumConverter);
            if (converter != null)
                converters.Remove(converter);

            converters.Freeze();

            return converters;

        }, true);

        private static JsonConverterCollection DefaultConverters
        {
            get { return defaultConverters.Value; }
        }
        
        private static JsonConverterCollection DefaultConvertersEnumsAsIntegers
        {
            get { return defaultConvertersEnumsAsIntegers.Value; }
        }
        


        /// <summary>
        /// Creates the serializer.
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
                TypeNameAssemblyFormat = FormatterAssemblyStyle.Simple,
                ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor,
                FloatParseHandling = FloatParseHandling.Double,
                Converters = new JsonConverterCollection()
            };

            CustomizeJsonSerializer(jsonSerializer);
            if (jsonSerializer.Converters.IsFrozen)  // if the user froze the collection, we don't need to do anything
                return jsonSerializer;
            var convertersToUse = SaveEnumsAsIntegers ? DefaultConvertersEnumsAsIntegers : DefaultConverters;
            if (jsonSerializer.Converters.Count == 0)
            {
                jsonSerializer.Converters = convertersToUse;
            }
            else
            {
                for (int i = convertersToUse.Count - 1; i >= 0; i--)
                {
                    jsonSerializer.Converters.Insert(0, convertersToUse[i]);
                }
            }
            return jsonSerializer;
        }

        /// <summary>
        /// Get the CLR type (if exists) from the document
        /// </summary>
        public string GetClrType(string id, RavenJObject document, RavenJObject metadata)
        {
            return FindClrType(id, document, metadata);
        }

        /// <summary>
        ///  Get the CLR type name to be stored in the entity metadata
        /// </summary>
        public string GetClrTypeName(Type entityType)
        {
            return FindClrTypeName(entityType);
        }

        /// <summary>
        /// When RavenDB needs to convert between a string id to a value type like int or guid, it calls
        /// this to perform the actual work
        /// </summary>
        public Func<object, string, string> FindIdValuePartForValueTypeConversion { get; set; }


        /// <summary>
        /// Translate the type tag name to the document key prefix
        /// </summary>
        public Func<string, string> TransformTypeTagNameToDocumentKeyPrefix { get; set; }

        ///<summary>
        /// Whether or not RavenDB will automatically enlist in distributed transactions
        ///</summary>
        public bool EnlistInDistributedTransactions { get; set; }

        /// <summary>
        /// Clone the current conventions to a new instance
        /// </summary>
        public DocumentConvention Clone()
        {
            return (DocumentConvention)MemberwiseClone();
        }

        /// <summary>
        /// This is called in order to ensure that reduce function in a sharded environment is run 
        /// over the merged results
        /// </summary>
        public ApplyReduceFunctionFunc ApplyReduceFunction { get; set; }


        /// <summary>
        /// This is called to provide replication behavior for the client. You can customize 
        /// this to inject your own replication / failover logic.
        /// </summary>
        public Func<string, HttpJsonRequestFactory, Func<string, IRequestTimeMetric>, IDocumentStoreReplicationInformer> ReplicationInformerFactory { get; set; }

#if !DNXCORE50
        /// <summary>
        ///  Attempts to prettify the generated linq expressions for indexes and transformers
        /// </summary>
        public bool PrettifyGeneratedLinqExpressions { get; set; }
#endif

        /// <summary>
        /// How index and transformer updates should be handled in replicated setup.
        /// Defaults to <see cref="Document.IndexAndTransformerReplicationMode"/>.
        /// </summary>
        public IndexAndTransformerReplicationMode IndexAndTransformerReplicationMode { get; set; }

        /// <summary>
        /// Controls whether properties on the object that weren't de-serialized to object properties 
        /// will be preserved when saving the document again. If false, those properties will be removed
        /// when the document will be saved.
        /// </summary>
        public bool PreserveDocumentPropertiesNotFoundOnModel { get; set; }

        public bool AcceptGzipContent { get; set; }

        public delegate bool TryConvertValueForQueryDelegate<in T>(string fieldName, T value, QueryValueConvertionType convertionType, out string strValue);

        private readonly List<Tuple<Type, TryConvertValueForQueryDelegate<object>>> listOfQueryValueConverters = new List<Tuple<Type, TryConvertValueForQueryDelegate<object>>>();
        private readonly Dictionary<string, SortOptions> customDefaultSortOptions = new Dictionary<string, SortOptions>();
        private readonly List<Type> customRangeTypes = new List<Type>();

        public void RegisterQueryValueConverter<T>(TryConvertValueForQueryDelegate<T> converter, SortOptions defaultSortOption = SortOptions.String, bool usesRangeField = false)
        {
            TryConvertValueForQueryDelegate<object> actual = (string name, object value, QueryValueConvertionType convertionType, out string strValue) =>
            {
                if (value is T)
                    return converter(name, (T)value, convertionType, out strValue);
                strValue = null;
                return false;
            };

            int index;
            for (index = 0; index < listOfQueryValueConverters.Count; index++)
            {
                var entry = listOfQueryValueConverters[index];
                if (entry.Item1.IsAssignableFrom(typeof(T)))
                {
                    break;
                }
            }

            listOfQueryValueConverters.Insert(index, Tuple.Create(typeof(T), actual));

            if (defaultSortOption != SortOptions.String)
                customDefaultSortOptions.Add(typeof(T).Name, defaultSortOption);

            if (usesRangeField)
                customRangeTypes.Add(typeof(T));
        }


        public bool TryConvertValueForQuery(string fieldName, object value, QueryValueConvertionType convertionType, out string strValue)
        {
            foreach (var queryValueConverterTuple in listOfQueryValueConverters
                    .Where(tuple => tuple.Item1.IsInstanceOfType(value)))
            {
                return queryValueConverterTuple.Item2(fieldName, value, convertionType, out strValue);
            }
            strValue = null;
            return false;
        }

        internal SortOptions? GetDefaultSortOption(Type type)
        {
            if (type == null)
                return null;

            var nonNullableType = (Nullable.GetUnderlyingType(type) ?? type);

            return GetDefaultSortOption(nonNullableType.Name);
        }

        public SortOptions GetDefaultSortOption(string typeName)
        {
            switch (typeName)
            {
                case "Int16":
                    return SortOptions.Short;
                case "Int32":
                    return SortOptions.Int;
                case "Int64":
                case "TimeSpan":
                    return SortOptions.Long;
                case "Double":
                case "Decimal":
                    return SortOptions.Double;
                case "Single":
                    return SortOptions.Float;
                case "String":
                    return SortOptions.String;
                default:
                    SortOptions value;
                    return customDefaultSortOptions.TryGetValue(typeName, out value)
                               ? value
                               : SortOptions.String;
            }
        }

        public bool UsesRangeType(object o)
        {
            if (o == null)
                return false;
            var type = o as Type ?? o.GetType();
            var nonNullable = Nullable.GetUnderlyingType(type);
            if (nonNullable != null)
                type = nonNullable;

            if (type == typeof(int) || type == typeof(long) || type == typeof(double) || type == typeof(float) ||
                type == typeof(decimal) || type == typeof(TimeSpan) || type == typeof(short))
                return true;

            return customRangeTypes.Contains(type);
        }

        protected Dictionary<Type, MemberInfo> idPropertyCache = new Dictionary<Type, MemberInfo>();

        /// <summary>
        /// Gets or sets the function to find the identity property.
        /// </summary>
        /// <value>The find identity property.</value>
        public Func<MemberInfo, bool> FindIdentityProperty { get; set; }

        /// <summary>
        /// Gets the identity property.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        public MemberInfo GetIdentityProperty(Type type)
        {
            MemberInfo info;
            var currentIdPropertyCache = idPropertyCache;
            if (currentIdPropertyCache.TryGetValue(type, out info))
                return info;

            var identityProperty = GetPropertiesForType(type).FirstOrDefault(FindIdentityProperty);

            if (identityProperty != null && identityProperty.DeclaringType != type)
            {
                var propertyInfo = identityProperty.DeclaringType.GetProperty(identityProperty.Name);
                identityProperty = propertyInfo ?? identityProperty;
    }

            idPropertyCache = new Dictionary<Type, MemberInfo>(currentIdPropertyCache)
            {
                {type, identityProperty}
            };

            return identityProperty;
        }

        private static IEnumerable<MemberInfo> GetPropertiesForType(Type type)
        {
            foreach (var propertyInfo in ReflectionUtil.GetPropertiesAndFieldsFor(type, BindingFlags.Public | BindingFlags.Instance | BindingFlags.NonPublic))
            {
                yield return propertyInfo;
            }

            foreach (var @interface in type.GetInterfaces())
            {
                foreach (var propertyInfo in GetPropertiesForType(@interface))
                {
                    yield return propertyInfo;
                }
            }
        }

    }

    [Flags]
    public enum IndexAndTransformerReplicationMode
    {
        /// <summary>
        /// No indexes or transformers are updated to replicated instances.
        /// </summary>
        None = 0,

        /// <summary>
        /// All indexes are replicated.
        /// </summary>
        Indexes = 2,

        /// <summary>
        /// All transformers are replicated.
        /// </summary>
        Transformers = 4,
    }

    public enum QueryValueConvertionType
    {
        Equality,
        Range
    }

    /// <summary>
    /// The consistency options for all queries, fore more details about the consistency options, see:
    /// http://www.allthingsdistributed.com/2008/12/eventually_consistent.html
    /// 
    /// Note that this option impact only queries, since we have Strong Consistency model for the documents
    /// </summary>
    public enum ConsistencyOptions
    {
        /// <summary>
        /// Ensures that after querying an index at time T, you will never see the results
        /// of the index at a time prior to T.
        /// This is ensured by the server, and require no action from the client
        /// </summary>
        None,
        /// <summary>
        ///  After updating a documents, will only accept queries which already indexed the updated value.
        /// </summary>
        [Obsolete("Beware of AlwaysWaitForNonStaleResultsAsOfLastWrite overuse. " +
                  "See: http://ravendb.net/docs/article-page/3.5/csharp/client-api/configuration/conventions/querying#defaultqueryingconsistency")]
        AlwaysWaitForNonStaleResultsAsOfLastWrite,
        /// <summary>
        /// Use AlwaysWaitForNonStaleResultsAsOfLastWrite, instead
        /// </summary>
        [Obsolete("Use AlwaysWaitForNonStaleResultsAsOfLastWrite, instead")]
        QueryYourWrites = AlwaysWaitForNonStaleResultsAsOfLastWrite,
        /// <summary>
        /// Use None, instead
        /// </summary>
        [Obsolete("Use None, instead")]
        MonotonicRead = None
    }
}
