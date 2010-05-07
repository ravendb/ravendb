using System;
using System.Reflection;
using Raven.Client.Util;
using System.Linq;

namespace Raven.Client.Document
{
	public class DocumentConvention
	{
		public DocumentConvention()
		{
			FindIdentityProperty = q => q.Name == "Id";
			FindTypeTagName = t => DefaultTypeTagName(t);
			DocumentKeyGenerator = entity => DefaultGenerateDocumentKey(this, entity);
		}

		public static string DefaultGenerateDocumentKey(DocumentConvention conventions, object entity)
		{
			return conventions.FindTypeTagName(entity.GetType()).ToLowerInvariant() + "/";
		}

		public static string DefaultTypeTagName(Type t)
		{
			return Inflector.Pluralize(t.Name);
		}

		public string GetTypeTagName(Type type)
		{
			return FindTypeTagName(type) ?? DefaultTypeTagName(type);
		}

		public string GenerateDocumentKey(object entity)
		{
			return DocumentKeyGenerator(entity);
		}

		public PropertyInfo GetIdentityProperty(Type type)
		{
			return type.GetProperties().FirstOrDefault(FindIdentityProperty);
		}

		public Func<Type, string> FindTypeTagName { private get; set; }
		public Func<PropertyInfo, bool> FindIdentityProperty { private get; set; }

		public Func<object, string> DocumentKeyGenerator { private get; set; }
	}
}