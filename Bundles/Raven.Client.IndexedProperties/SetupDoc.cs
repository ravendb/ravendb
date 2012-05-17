using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Client.IndexedProperties
{
	public class SetupDoc
	{
		public static string IdPrefix = "Raven/IndexedProperties/";
		public string DocumentKey { get; set; }
		public IList<Tuple<string, string>> FieldNameMappings { get; set; }
	}
}
