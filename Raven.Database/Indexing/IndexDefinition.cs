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
	/// <summary>
	/// A definition of a RavenIndex
	/// </summary>
	public class IndexDefinition
	{
		/// <summary>
		/// Gets or sets the map function
		/// </summary>
		/// <value>The map.</value>
		public string Map { get; set; }

		/// <summary>
		/// Gets or sets the reduce function
		/// </summary>
		/// <value>The reduce.</value>
		public string Reduce { get; set; }

        /// <summary>
        /// Gets or sets the translator function
        /// </summary>
        public string Translator { get; set; }

		/// <summary>
		/// Gets a value indicating whether this instance is map reduce index definition
		/// </summary>
		/// <value>
		/// 	<c>true</c> if this instance is map reduce; otherwise, <c>false</c>.
		/// </value>
		public bool IsMapReduce
		{
			get { return Reduce != null; }
		}

        internal bool IsCompiled { get; set; }

		/// <summary>
		/// Gets or sets the stores options
		/// </summary>
		/// <value>The stores.</value>
		public IDictionary<string, FieldStorage> Stores { get; set; }

		/// <summary>
		/// Gets or sets the indexing options
		/// </summary>
		/// <value>The indexes.</value>
		public IDictionary<string, FieldIndexing> Indexes { get; set; }

		/// <summary>
		/// Gets or sets the sort options.
		/// </summary>
		/// <value>The sort options.</value>
		public IDictionary<string, SortOptions> SortOptions { get; set; }

		/// <summary>
		/// Gets or sets the analyzers options
		/// </summary>
		/// <value>The analyzers.</value>
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
				if (!name.EndsWith("_Range"))
				{
					return null;
				}
				var nameWithoutRange = name.Substring(0, name.Length - "_Range".Length);
				if (!SortOptions.TryGetValue(nameWithoutRange, out value))
					return null;
			}
			return value;
		}

		[CLSCompliant(false)]
		public Field.Index GetIndex(string name, Field.Index defaultIndex)
		{
			if (Indexes == null)
				return defaultIndex;
			FieldIndexing value;
			if (Indexes.TryGetValue(name, out value) == false)
			{
			    string ignored;
			    if(Analyzers.TryGetValue(name, out ignored))
                    return Field.Index.ANALYZED;// if there is a custom analyzer, the value should be analyzer
			    return defaultIndex;
			}
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

		/// <summary>
		/// Initializes a new instance of the <see cref="IndexDefinition"/> class.
		/// </summary>
		public IndexDefinition()
		{
			Indexes = new Dictionary<string, FieldIndexing>();
			Stores = new Dictionary<string, FieldStorage>();
			Analyzers = new Dictionary<string, string>();
			SortOptions = new Dictionary<string, SortOptions>();
		}

		/// <summary>
		/// Equalses the specified other.
		/// </summary>
		/// <param name="other">The other.</param>
		/// <returns></returns>
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

		/// <summary>
		/// Determines whether the specified <see cref="System.Object"/> is equal to this instance.
		/// </summary>
		/// <param name="obj">The <see cref="System.Object"/> to compare with this instance.</param>
		/// <returns>
		/// 	<c>true</c> if the specified <see cref="System.Object"/> is equal to this instance; otherwise, <c>false</c>.
		/// </returns>
		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			return Equals(obj as IndexDefinition);
		}

		/// <summary>
		/// Returns a hash code for this instance.
		/// </summary>
		/// <returns>
		/// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. 
		/// </returns>
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