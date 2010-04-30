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

		public Dictionary<string, FieldStorage> Stores { get; set; }

		public Dictionary<string, FieldIndexing> Indexes { get; set; }

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
				return Field.Index.TOKENIZED;
			FieldIndexing value;
			if (Indexes.TryGetValue(name, out value) == false)
				return Field.Index.TOKENIZED;
			switch (value)
			{
				case FieldIndexing.No:
					return Field.Index.NO;
				case FieldIndexing.NoNorms:
					return Field.Index.NO_NORMS;
				case FieldIndexing.Tokenized:
					return Field.Index.TOKENIZED;
				case FieldIndexing.Untokenized:
					return Field.Index.UN_TOKENIZED;
				default:
					throw new ArgumentOutOfRangeException();
			}
		}

		public IndexDefinition()
		{
			Indexes = new Dictionary<string, FieldIndexing>();
			Stores = new Dictionary<string, FieldStorage>();
		}
	}
}