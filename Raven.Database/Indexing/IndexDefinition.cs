using System;
using System.Collections.Generic;
using Lucene.Net.Documents;

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
	}
}