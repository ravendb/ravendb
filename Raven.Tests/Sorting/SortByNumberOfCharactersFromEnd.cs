// -----------------------------------------------------------------------
//  <copyright file="SortByNumberOfCharactersFromEnd.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Abstractions.Data;
using Raven.Database.Indexing.Sorting.Custom;

namespace Raven.Tests.Sorting
{
	public class SortByNumberOfCharactersFromEnd : IndexEntriesToComparablesGenerator
	{
		private readonly int len;

		public SortByNumberOfCharactersFromEnd(IndexQuery indexQuery) : base(indexQuery)
		{
			len = IndexQuery.TransformerParameters["len"].Value<int>();
		}

		public override IComparable Generate(IndexReader reader, int doc)
		{
			var document = reader.Document(doc);
			var name = document.GetField("Name").StringValue;
			return name.Substring(name.Length - len, len);
		}
	}
}