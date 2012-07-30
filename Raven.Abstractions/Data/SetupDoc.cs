using System;
using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
	public class IndexedPropertiesSetupDoc
	{
		public static string IdPrefix = "Raven/IndexedProperties/";
		public string DocumentKey { get; set; }
		public IDictionary<string,string> FieldNameMappings { get; set; }

		public IndexedPropertiesSetupDoc()
		{
			FieldNameMappings = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase);
		}
	}
}
