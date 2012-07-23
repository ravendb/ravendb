using System.Collections.Generic;

#if !CLIENT
namespace Raven.Bundles.IndexedProperties
#else
namespace Raven.Client.IndexedProperties
#endif
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
