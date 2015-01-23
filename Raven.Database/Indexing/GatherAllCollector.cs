//-----------------------------------------------------------------------
// <copyright file="GatherAllCollector.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Abstractions.Data;

namespace Raven.Database.Indexing
{
	
	public class GatherAllCollector : Collector
	{
		private int _docBase;
        private readonly List<int> documents = new List<int>();

		public override void SetScorer(Scorer scorer)
		{
		}

		public override void Collect(int doc)
		{
			documents.Add(doc + _docBase);
		}

		public override void SetNextReader(IndexReader reader, int docBase)
		{
			_docBase = docBase;
		}

		public override bool AcceptsDocsOutOfOrder
		{
			get { return true; }
		}

        public List<int> Documents
		{
			get { return documents; }
		}

		public TopDocs ToTopDocs()
		{
			return new TopDocs(documents.Count, documents.Select(i => new ScoreDoc(i, 0)).ToArray(), 0);
		}
	}
}
