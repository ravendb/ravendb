//-----------------------------------------------------------------------
// <copyright file="IndexDefinition.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;

namespace Raven.Database.Indexing
{
	/// <summary>
	/// A definition of a RavenIndex
	/// </summary>
	public class IndexDefinition
	{
		/// <summary>
		/// Get or set the name of the index
		/// </summary>
		public string Name { get; set; }

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
		public string TransformResults { get; set; }

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

		public bool IsCompiled { get; set; }

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

		/// <summary>
		/// The fields that are queryable in the index
		/// </summary>
		public IList<string> Fields {get;set;}

		/// <summary>
		/// Initializes a new instance of the <see cref="IndexDefinition"/> class.
		/// </summary>
		public IndexDefinition()
		{
			Indexes = new Dictionary<string, FieldIndexing>();
			Stores = new Dictionary<string, FieldStorage>();
			Analyzers = new Dictionary<string, string>();
			SortOptions = new Dictionary<string, SortOptions>();
			Fields = new List<string>();
		}

		/// <summary>
		/// Equals the specified other.
		/// </summary>
		/// <param name="other">The other.</param>
		/// <returns></returns>
		public bool Equals(IndexDefinition other)
		{
			if (ReferenceEquals(null, other)) return false;
			if (ReferenceEquals(this, other)) return true;
			return Equals(other.Map, Map) && 
				Equals(other.Name, Name) &&
				Equals(other.Reduce, Reduce) && 
				Equals(other.TransformResults, TransformResults) && 
				DictionaryEquals(other.Stores, Stores) &&
				DictionaryEquals(other.Indexes, Indexes) &&
				DictionaryEquals(other.Analyzers, Analyzers) &&
				DictionaryEquals(other.SortOptions, SortOptions);
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

		private static int DictionaryHashCode<TKey, TValue>(IEnumerable<KeyValuePair<TKey, TValue>> x)
		{
			int result = 0;
			foreach (var kvp in x)
			{
				result = (result * 397) ^ kvp.Key.GetHashCode();
				result = (result*397) ^ (!Equals(kvp.Value, default(TValue)) ? kvp.Value.GetHashCode() : 0);
			}
			return result;
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
				result = (result*397) ^ DictionaryHashCode(Stores);
				result = (result*397) ^ DictionaryHashCode(Indexes);
				result = (result*397) ^ DictionaryHashCode(Analyzers);
				result = (result*397) ^ DictionaryHashCode(SortOptions);
				return result;
			}
		}

		public string Type
		{
			get
			{
				var name = Name ?? string.Empty;
				if (name.StartsWith("Temp")) return "Temp";
				if (name.StartsWith("Auto")) return "Auto";
				if (IsCompiled) return "Compiled";
				if (IsMapReduce) return "MapReduce";
				return "Map";
			}
		}
	}
}
