using System;
using System.Reflection;

namespace Raven.Client.Document
{
	public class DocumentConvention
	{
		public DocumentConvention()
		{
			FindIdentityProperty = q => q.Name == "Id";
		}

		public Func<PropertyInfo, bool> FindIdentityProperty { get; set; }
	}
}