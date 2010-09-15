using System;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization.Formatters;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using Raven.Client.Util;
using System.Linq;
using Raven.Database.Json;

namespace Raven.Client.Document
{
	/// <summary>
	/// The set of conventions used by the <see cref="DocumentStore"/> which allow the users to customize
	/// the way the Raven client API behaves
	/// </summary>
	public class DocumentConvention
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="DocumentConvention"/> class.
		/// </summary>
		public DocumentConvention()
		{
			FindIdentityProperty = q => q.Name == "Id";
			FindTypeTagName = t => DefaultTypeTagName(t);
			IdentityPartsSeparator = "/";
			JsonContractResolver = new DefaultRavenContractResolver(shareCache: true)
			{
				DefaultMembersSearchFlags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance
			};
		    MaxNumberOfRequestsPerSession = 30;
			CustomizeJsonSerializer = serializer => { };
		}

		/// <summary>
		/// Register an action to customize the json serializer used by the <see cref="DocumentStore"/>
		/// </summary>
		public Action<JsonSerializer> CustomizeJsonSerializer { get; set; }

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
		/// Generates the document key using identity.
		/// </summary>
		/// <param name="conventions">The conventions.</param>
		/// <param name="entity">The entity.</param>
		/// <returns></returns>
		public static string GenerateDocumentKeyUsingIdentity(DocumentConvention conventions, object entity)
		{
			return conventions.FindTypeTagName(entity.GetType()).ToLowerInvariant() + "/";
		}

		/// <summary>
		/// Get the default tag name for the specified type.
		/// </summary>
		/// <param name="t">The t.</param>
		/// <returns></returns>
		public static string DefaultTypeTagName(Type t)
		{
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
				return sb.ToString();
			}
			return Inflector.Pluralize(t.Name);
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
			return type.GetProperties().FirstOrDefault(FindIdentityProperty);
		}

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
		/// Gets or sets the function to find the identity property.
		/// </summary>
		/// <value>The find identity property.</value>
		public Func<PropertyInfo, bool> FindIdentityProperty { get; set; }

		/// <summary>
		/// Gets or sets the document key generator.
		/// </summary>
		/// <value>The document key generator.</value>
		public Func<object, string> DocumentKeyGenerator { get; set; }

		/// <summary>
		/// Creates the serializer.
		/// </summary>
		/// <returns></returns>
		public JsonSerializer CreateSerializer()
		{
			var jsonSerializer = new JsonSerializer
			{
				ContractResolver = JsonContractResolver,
				TypeNameHandling = TypeNameHandling.Auto,
				TypeNameAssemblyFormat = FormatterAssemblyStyle.Simple,
				ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor,
				Converters =
					{
						new JsonEnumConverter(),
						new JsonLuceneDateTimeConverter(),
#if !NET_3_5
						new JsonDynamicConverter()
#endif
					}
			};
			CustomizeJsonSerializer(jsonSerializer);
			return jsonSerializer;
		}
	}

	/// <summary>
	/// The default json contract will serialize all properties and all public fields
	/// </summary>
	public class DefaultRavenContractResolver : DefaultContractResolver
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="DefaultRavenContractResolver"/> class.
		/// </summary>
		/// <param name="shareCache">If set to <c>true</c> the <see cref="T:Newtonsoft.Json.Serialization.DefaultContractResolver"/> will use a cached shared with other resolvers of the same type.
		/// Sharing the cache will significantly performance because expensive reflection will only happen once but could cause unexpected
		/// behavior if different instances of the resolver are suppose to produce different results. When set to false it is highly
		/// recommended to reuse <see cref="T:Newtonsoft.Json.Serialization.DefaultContractResolver"/> instances with the <see cref="T:Newtonsoft.Json.JsonSerializer"/>.</param>
		public DefaultRavenContractResolver(bool shareCache) : base(shareCache)
		{
		}

		protected override System.Collections.Generic.List<MemberInfo> GetSerializableMembers(Type objectType)
		{
			var serializableMembers = base.GetSerializableMembers(objectType);
			foreach (var toRemove in serializableMembers
				.Where(MembersToFilterOut)
				.ToArray())
			{
				serializableMembers.Remove(toRemove);
			}
			return serializableMembers;
		}

		private static bool MembersToFilterOut(MemberInfo info)
		{
			if (info is EventInfo)
				return true;
			var fieldInfo = info as FieldInfo;
			if (fieldInfo != null && !fieldInfo.IsPublic)
				return true;
			return info.GetCustomAttributes(typeof(CompilerGeneratedAttribute),true).Length > 0;
		}
	}
}