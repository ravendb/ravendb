//-----------------------------------------------------------------------
// <copyright file="GatherAllCollector.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;

using Lucene.Net.Index;
using Lucene.Net.Search;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Collectors
{
    public class GatherAllCollector : Collector
    {
        private int _docBase;

        public override void SetScorer(Scorer scorer)
        {
        }

        public override void Collect(int doc)
        {
            Documents.Add(doc + _docBase);
        }

        public override void SetNextReader(IndexReader reader, int docBase)
        {
            _docBase = docBase;
        }

        public override bool AcceptsDocsOutOfOrder => true;

        public List<int> Documents { get; } = new List<int>();

        public TopDocs ToTopDocs()
        {
            return new TopDocs(Documents.Count, Documents.Select(i => new ScoreDoc(i, 0)).ToArray(), 0);
        }
    }
}
