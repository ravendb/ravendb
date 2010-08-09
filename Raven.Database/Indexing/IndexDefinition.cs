using System;
using System.Collections;
using System.Collections.Generic;
#if !CLIENT
using System.Linq.Expressions;
using System.Text.RegularExpressions;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Search;
using Version = Lucene.Net.Util.Version;

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

        internal bool IsCompiled { get; set; }

		public IDictionary<string, FieldStorage> Stores { get; set; }

		public IDictionary<string, FieldIndexing> Indexes { get; set; }

		public IDictionary<string, SortOptions> SortOptions { get; set; }

		public IDictionary<string, string> Analyzers { get; set; }
		
#if !CLIENT
		[CLSCompliant(false)]
		public Analyzer GetAnalyzer(string name)
		{
			if (Analyzers == null)
				return null;
			string analyzerTypeAsString;
			if(Analyzers.TryGetValue(name, out analyzerTypeAsString) == false)
				return null;
			return CreateAnalyzerInstance(name, analyzerTypeAsString);
		}

		[CLSCompliant(false)]
		public Analyzer CreateAnalyzerInstance(string name, string analyzerTypeAsString)
		{
			var analyzerType = typeof (StandardAnalyzer).Assembly.GetType(analyzerTypeAsString) ??
				Type.GetType(analyzerTypeAsString);
			if(analyzerType == null)
				throw new InvalidOperationException("Cannot find type '" + analyzerTypeAsString + "' for field: " + name);
			return (Analyzer) Activator.CreateInstance(analyzerType);
		}

		[CLSCompliant(false)]
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

		public SortOptions? GetSortOption(string name)
		{
			SortOptions value;
			if (!SortOptions.TryGetValue(name, out value))
			{
				return null;
			}
			return value;
		}


		public SortField GetSort(string name)
		{
			SortOptions value;
			if (!SortOptions.TryGetValue(name, out value))
			{
				return null;
			}
			switch (value)
			{
				case Indexing.SortOptions.String:
					return new SortField(name, SortField.STRING);
				case Indexing.SortOptions.Int:
					return new SortField(name, SortField.INT);
				case Indexing.SortOptions.Float:
					return new SortField(name, SortField.FLOAT);
				case Indexing.SortOptions.Long:
					return new SortField(name, SortField.LONG);
				case Indexing.SortOptions.Double:
					return new SortField(name, SortField.DOUBLE);
				case Indexing.SortOptions.Short:
					return new SortField(name, SortField.SHORT);
				case Indexing.SortOptions.Custom:
					return new SortField(name, SortField.CUSTOM);
				case Indexing.SortOptions.Byte:
					return new SortField(name, SortField.BYTE);
				case Indexing.SortOptions.StringVal:
					return new SortField(name, SortField.STRING_VAL);
				default:
					throw new ArgumentOutOfRangeException();
			}
		}

		[CLSCompliant(false)]
		public Field.Index GetIndex(string name, Field.Index defaultIndex)
		{
			if (Indexes == null)
				return defaultIndex;
			FieldIndexing value;
			if (Indexes.TryGetValue(name, out value) == false)
				return defaultIndex;
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
			Analyzers = new Dictionary<string, string>();
			SortOptions = new Dictionary<string, SortOptions>();
		}

		public bool Equals(IndexDefinition other)
		{
			if (ReferenceEquals(null, other)) return false;
			if (ReferenceEquals(this, other)) return true;
			return Equals(other.Map, Map) && Equals(other.Reduce, Reduce) && DictionaryEquals(other.Stores, Stores) &&
				DictionaryEquals(other.Indexes, Indexes);
		}

		private static bool DictionaryEquals<TKey,TValue>(IDictionary<TKey, TValue> x, IDictionary<TKey, TValue> y)
		{
			if(x.Count!=y.Count)
				return false;
			foreach (var v in x)
			{
				TValue value;
				if(y.TryGetValue(v.Key, out value) == false)
					return false;
				if(Equals(value,v.Value)==false)
					return false;
			}
			return true;
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			return Equals(obj as IndexDefinition);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				int result = (Map != null ? Map.GetHashCode() : 0);
				result = (result*397) ^ (Reduce != null ? Reduce.GetHashCode() : 0);
				result = (result*397) ^ (Stores != null ? Stores.GetHashCode() : 0);
				result = (result*397) ^ (Indexes != null ? Indexes.GetHashCode() : 0);
				return result;
			}
		}
	}
}