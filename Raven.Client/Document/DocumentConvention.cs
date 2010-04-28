using System;
using System.Reflection;
using Raven.Client.Util;

namespace Raven.Client.Document
{
	public class DocumentConvention
	{
		public DocumentConvention()
		{
			FindIdentityProperty = q => q.Name == "Id";
			FindTypeTagName = t => Inflector.Pluralize(t.Name);
			GenerateDocumentKey = entity => FindTypeTagName(entity.GetType()).ToLowerInvariant() + "/";
		}

		public Func<Type, string> FindTypeTagName { get; set; }
		public Func<PropertyInfo, bool> FindIdentityProperty { get; set; }

		public Func<object, string> GenerateDocumentKey { get; set; }
	}
}