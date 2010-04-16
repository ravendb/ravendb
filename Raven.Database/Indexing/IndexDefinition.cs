using System.Collections.Generic;

namespace Raven.Database.Indexing
{
	public class IndexDefinition
	{
		public string Map { get; set; }
		
		public string Reduce { get; set; }

		public bool IsMapReduce
		{
			get { return Reduce != null; }
		}

		public Dictionary<string, FieldStorage> Stores { get; set; }

		public Dictionary<string, FieldIndexing> Indexes { get; set; }
	}
}