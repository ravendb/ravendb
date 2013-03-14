using System.Collections.Generic;

namespace Raven.Database.Bundles.SqlReplication
{
	public class ConversionScriptResult
	{
		private readonly List<string> tablesInOrder = new List<string>();
		private readonly Dictionary<string, List<ItemToReplicate>> data = new Dictionary<string, List<ItemToReplicate>>();

		public List<string> TablesInOrder
		{
			get { return tablesInOrder; }
		}

		public Dictionary<string, List<ItemToReplicate>> Data
		{
			get { return data; }
		}

		public void AddTable(string table)
		{
			if (data.ContainsKey(table) == false)
				tablesInOrder.Add(table);
		}
	}
}