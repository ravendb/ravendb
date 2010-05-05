using System;
using System.Collections.Generic;
#if !CLIENT
using Lucene.Net.Documents;
#endif

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

#if !CLIENT

		public Field.Store GetStorage(string name, Field.Store defaultStorage)
		{
			if(Stores == null)
				return defaultStorage;
			FieldStorage value;
			if (Stores.TryGetValue(name, out value) == false)
				return defaultStorage;
			switch (value)
			{
				case FieldStorage.Yes:
					return Field.Store.YES;
				case FieldStorage.No:
					return Field.Store.NO;
				case FieldStorage.Compress:
					return Field.Store.COMPRESS;
				default:
					throw new ArgumentOutOfRangeException();
			}
		}

		public Field.Index GetIndex(string name)
		{
			if (Indexes == null)
				return Field.Index.ANALYZED;
			FieldIndexing value;
			if (Indexes.TryGetValue(name, out value) == false)
				return Field.Index.ANALYZED;
			switch (value)
			{
				case FieldIndexing.No:
					return Field.Index.NO;
				case FieldIndexing.NotAnalyzedNoNorms:
					return Field.Index.NOT_ANALYZED_NO_NORMS;
				case FieldIndexing.Analyzed:
					return Field.Index.ANALYZED;
				case FieldIndexing.NotAnalyzed:
					return Field.Index.NOT_ANALYZED;
				default:
					throw new ArgumentOutOfRangeException();
			}
		}
#endif
		public IndexDefinition()
		{
			Indexes = new Dictionary<string, FieldIndexing>();
			Stores = new Dictionary<string, FieldStorage>();
		}
	}
}