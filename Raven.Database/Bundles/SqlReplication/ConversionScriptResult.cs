using System.Collections.Generic;

namespace Raven.Database.Bundles.SqlReplication
{
	public class ConversionScriptResult
	{
		public readonly Dictionary<string, List<ItemToReplicate>> Data = new Dictionary<string, List<ItemToReplicate>>();
	    public readonly List<string> Ids = new List<string>(); 
	}
}