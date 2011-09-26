//-----------------------------------------------------------------------
// <copyright file="DocumentConvention.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Net;
using System.Reflection;
using System.Runtime.Serialization.Formatters;
using System.Text;
#if !NET_3_5
using System.Threading.Tasks;
#endif
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using Raven.Abstractions;
using Raven.Abstractions.Json;
using Raven.Client.Connection;
using Raven.Client.Converters;
using Raven.Client.Util;
using System.Linq;
using Raven.Json.Linq;


namespace Raven.Client.Document
{
	/// <summary>
	/// The set of conventions used by the <see cref="DocumentStore"/> which allow the users to customize
	/// the way the Raven client API behaves
	/// </summary>
	public class DocumentConvention
	{
		private Dictionary<Type, PropertyInfo> idPropertyCache = new Dictionary<Type, PropertyInfo>();

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
			UseParallelMultiGet = true;
			DefaultQueryingConsistency = ConsistencyOptions.MonotonicRead;
			FailoverBehavior = FailoverBehavior.AllowReadsFromSecondaries;
			ShouldCacheRequest = url => true;
			FindIdentityProperty = q => q.Name == "Id";
			FindClrType = (id, doc, metadata) => metadata.Value<string>(Abstractions.Data.Constants.RavenClrType);

#if !SILVERLIGHT
			FindClrTypeName = entityType => ReflectionUtil.GetFullNameWithoutVersionInformation(entityType);
#else
			FindClrTypeName = entityType => entityType.AssemblyQualifiedName;
#endif

			FindFullDocumentKeyFromNonStringIdentifier = DefaultFindFullDocumentKeyFromNonStringIdentifier;
			FindIdentityPropertyNameFromEntityName = entityName => "Id";
			FindTypeTagName = DefaultTypeTagName;
			FindPropertyNameForIndex = (indexedType, indexedName, path, prop) => prop;
			FindPropertyNameForDynamicIndex = (indexedType, indexedName, path, prop) => path + prop;
			IdentityPartsSeparator = "/";
			JsonContractResolver = new DefaultRavenContractResolver(shareCache: true)
			{
				DefaultMembersSearchFlags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance
			};
			MaxNumberOfRequestsPerSession = 30;
			CustomizeJsonSerializer = serializer => { };
		}

		///<summary>
		/// Find the full document name assuming that we are using the standard conventions
		/// for generating a document key
		///</summary>
		///<returns></returns>
		public string DefaultFindFullDocumentKeyFromNonStringIdentifier(object id, Type type, bool allowNull)
		{
			var converter = IdentityTypeConvertors.FirstOrDefault(x=>x.CanConvertFrom(id.GetType()));
			var tag = GetTypeTagName(type);
			if (tag != null)
			{
				tag += IdentityPartsSeparator;
				tag = tag.ToLowerInvariant();
			}
			if (converter != null)
			{
				return converter.ConvertFrom(tag, id, allowNull);
			}
			return tag + id;
		}


		/// <summary>
		/// How should we behave in a replicated environment when we can't 
		/// reach the primary node and need to failover to secondary node(s).
		/// </summary>
		public FailoverBehavior FailoverBehavior { get; set; }

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
		/// Gets or sets the identity parts separator used by the hilo generators
		/// </summary>
		/// <value>The identity parts separator.</value>
		public string IdentityPartsSeparator { get; set; }

		/// <summary>
		/// Gets or sets the default max number of requests per session.
		/// </summary>
		/// <value>The max number of requests per session.</value>
		public int MaxNumberOfRequestsPerSession { get; set; }

		/// <summary>
		/// The consistency options used when querying the database by default
		/// Note that this option impact only queries, since we have Strong Consistency model for the documents
		/// </summary>
		public ConsistencyOptions DefaultQueryingConsistency { get; set; }

		/// <summary>
		/// Generates the document key using identity.
		/// </summary>
		/// <param name="conventions">The conventions.</param>
		/// <param name="entity">The entity.</param>
		/// <returns></returns>
		public static string GenerateDocumentKeyUsingIdentity(DocumentConvention conventions, object entity)
		{
			return conventions.FindTypeTagName(entity.GetType()).ToLower() + "/";
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
			if(t.IsGenericType)
			{
				var name = t.GetGenericTypeDefinition().Name;
				if(name.Contains("`"))
				{
					name = name.Substring(0, name.IndexOf("`"));
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
		/// Generates the document key.
		/// </summary>
		/// <param name="entity">The entity.</param>
		/// <returns></returns>
		public string GenerateDocumentKey(object entity)
		{
			return DocumentKeyGenerator(entity);
		}

		/// <summary>
		/// Gets the identity property.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <returns></returns>
		public PropertyInfo GetIdentityProperty(Type type)
		{
			PropertyInfo info;
			var currentIdPropertyCache = idPropertyCache;
			if (currentIdPropertyCache.TryGetValue(type, out info))
				return info;

			var identityProperty = type.GetProperties().FirstOrDefault(FindIdentityProperty);

			if (identityProperty!= null && identityProperty.DeclaringType != type)
			{
				var propertyInfo = identityProperty.DeclaringType.GetProperty(identityProperty.Name);
				identityProperty = propertyInfo ?? identityProperty;
			}

			idPropertyCache = new Dictionary<Type, PropertyInfo>(currentIdPropertyCache)
			{
				{type, identityProperty}
			};

			return identityProperty;
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
		/// Whatever or not RavenDB should cache the request to the specified url.
		/// </summary>
		public Func<string, bool> ShouldCacheRequest { get; set; }

		/// <summary>
		/// Gets or sets the function to find the identity property.
		/// </summary>
		/// <value>The find identity property.</value>
		public Func<PropertyInfo, bool> FindIdentityProperty { get; set; }

		/// <summary>
		/// Get or sets the function to get the identity property name from the entity name
		/// </summary>
		public Func<string, string> FindIdentityPropertyNameFromEntityName { get; set; }

		/// <summary>
		/// Gets or sets the document key generator.
		/// </summary>
		/// <value>The document key generator.</value>
		public Func<object, string> DocumentKeyGenerator { get; set; }

		/// <summary>
		/// Instruct RavenDB to parallel Multi Get processing 
		/// when handling lazy requests
		/// </summary>
		public bool UseParallelMultiGet { get; private set; }

		/// <summary>
		/// Creates the serializer.
		/// </summary>
		/// <returns></returns>
		public JsonSerializer CreateSerializer()
		{
			var jsonSerializer = new JsonSerializer
			{
				ObjectCreationHandling = ObjectCreationHandling.Replace,
				ContractResolver = JsonContractResolver,
				TypeNameHandling = TypeNameHandling.Auto,
				TypeNameAssemblyFormat = FormatterAssemblyStyle.Simple,
				ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor,
				Converters =
					{
						new JsonLuceneDateTimeConverter(),
						new JsonFloatConverter(),
						new JsonNumericConverter<int>(int.TryParse),
						new JsonNumericConverter<long>(long.TryParse),
						new JsonNumericConverter<decimal>(decimal.TryParse),
						new JsonNumericConverter<double>(double.TryParse),
						new JsonNumericConverter<short>(short.TryParse),
						new JsonMultiDimensionalArrayConverter(),
#if !NET_3_5 && !SILVERLIGHT
						new JsonDynamicConverter()
#endif
					}
			};

			for (var i = Default.Converters.Length -1; i >= 0; i--)
			{
				jsonSerializer.Converters.Insert(0, Default.Converters[i]);
			}

			CustomizeJsonSerializer(jsonSerializer);
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
		/// Handles unauthenticate responses, usually by authenticating against the oauth server
		/// </summary>
		public Func<HttpWebResponse, Action<HttpWebRequest>> HandleUnauthorizedResponse { get; set; }

#if !NET_3_5
		/// <summary>
		/// Begins handling of unauthenticate responses, usually by authenticating against the oauth server
		/// in async manner
		/// </summary>
		public Func<HttpWebResponse, Task<Action<HttpWebRequest>>> HandleUnauthorizedResponseAsync { get; set; }
#endif 
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
		MonotonicRead,
		/// <summary>
		///  After updating a documents, will only accept queries which already indexed the updated value.
		/// </summary>
		QueryYourWrites,
	}
}
