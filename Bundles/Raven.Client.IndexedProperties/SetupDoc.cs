using System;
using System.Collections.Generic;

namespace Raven.Client.IndexedProperties
{
	public class SetupDoc
	{
		public static string IdPrefix = "Raven/IndexedProperties/";
		public string DocumentKey { get; set; }
		public IDictionary<string,string> FieldNameMappings { get; set; }

		public SetupDoc()
		{
			FieldNameMappings = new Dictionary<string, string>();
		}
	}
}
