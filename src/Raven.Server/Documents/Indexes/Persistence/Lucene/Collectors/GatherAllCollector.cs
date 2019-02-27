//-----------------------------------------------------------------------
// <copyright file="GatherAllCollector.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Sparrow;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Collectors
{
    public sealed class GatherAllCollector : Collector, IDisposable
    {       
        private int _docBase;
        private readonly List<ScoreDoc> _docs;

        private Scorer _scorer;
        private float _maxScore;

        public GatherAllCollector(int numberOfDocsToCollect)
        {
            _docs = CollectorsPool.Instance.Allocate();
        }

        public override void SetScorer(Scorer scorer)
        {
            _scorer = scorer;
        }

        public override void Collect(int doc, IState state)
        {
            var score = _scorer?.Score(state) ?? 0;
            if (score > _maxScore)
                _maxScore = score;

            _docs.Add(new ScoreDoc(doc + _docBase, score));
        }

        public override void SetNextReader(IndexReader reader, int docBase, IState state)
        {
            _docBase = docBase;
        }

        public override bool AcceptsDocsOutOfOrder => true;

        public TopDocs ToTopDocs()
        {
            return new TopDocs(_docs.Count, _docs.ToArray(), _maxScore);
        }

        public void Dispose()
        {
            _docs.Clear();
            CollectorsPool.Instance.Free(_docs);
        }
    }
}
