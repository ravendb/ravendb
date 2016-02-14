using System;
using System.Collections.Generic;
using Tryouts.Corax;
using Voron.Impl;

namespace Corax.Queries
{
	public abstract class Query
	{
		protected FullTextIndex Index;
		protected Transaction Transaction;
		protected IndexingConventions.ScorerCalc Score;

		public float Boost { get; set; }

		protected Query()
		{
			Boost = 1.0f;
		}
		
		public void Initialize(FullTextIndex index, Transaction tx, IndexingConventions.ScorerCalc score)
		{
			Index = index;
			Transaction = tx;
			Score = score;
			Init();
		}

		protected abstract void Init();
		public abstract IEnumerable<QueryMatch> Execute();

		public abstract override string ToString();
	}
}