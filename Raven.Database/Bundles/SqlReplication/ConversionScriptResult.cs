using System.Collections.Generic;

namespace Raven.Database.Bundles.SqlReplication
{
	public class ConversionScriptResult
	{
		private readonly Dictionary<string, List<ItemToReplicate>> data = new Dictionary<string, List<ItemToReplicate>>();

		public Dictionary<string, List<ItemToReplicate>> Data
		{
			get { return data; }
		}
	}
}