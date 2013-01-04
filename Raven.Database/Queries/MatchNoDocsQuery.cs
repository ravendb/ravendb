//-----------------------------------------------------------------------
// <copyright file="AbstractIndexCreationTask.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using Lucene.Net.Index;
using Lucene.Net.Search;

namespace Raven.Database.Queries
{
	public sealed class MatchNoDocsQuery : Query
	{
		public static MatchNoDocsQuery INSTANCE = new MatchNoDocsQuery();

		/**
		 * Since all instances of this class are equal to each other,
		 * we have a constant hash code.
		 */
		private const int HASH_CODE = 12345;

		/// <summary>
		/// Weight implementation that matches no documents.
		/// </summary>
		private class MatchNoDocsWeight : Weight
		{
			private readonly MatchNoDocsQuery enclosingInstance;
			private readonly Lucene.Net.Search.Similarity similarity;

			public MatchNoDocsWeight(Searcher searcher, MatchNoDocsQuery enclosingInstance)
			{
				this.enclosingInstance = enclosingInstance;
				this.similarity = searcher.Similarity;
			}

			public override string ToString()
			{
				return "weight(" + enclosingInstance + ")";
			}

			public override Query Query
			{
				get
				{
					return enclosingInstance;
				}
			}

			public override float Value
			{
				get { return 0; }
			}

			public override float GetSumOfSquaredWeights()
			{
				return 0;
			}

			public override void Normalize(float queryNorm)
			{
			}

			public override Scorer Scorer(IndexReader reader,
								 bool scoreDocsInOrder,
								 bool topScorer)
			{
				return null;
			}

			public override Explanation Explain(IndexReader reader, int doc)
			{
				return new ComplexExplanation(false, 0, "MatchNoDocs matches nothing");
			}
		}

		public override Weight CreateWeight(Searcher searcher)
		{
			return new MatchNoDocsWeight(searcher, this);
		}

		public override void ExtractTerms(ISet<Term> terms)
		{
		}


		public override string ToString(string field)
		{
			return "MatchNoDocsQuery";
		}

		public override int GetHashCode()
		{
			return HASH_CODE;
		}

		public override bool Equals(object obj)
		{
			return obj is MatchNoDocsQuery;
		}

		public override object Clone()
		{
			return INSTANCE;
		}
	}
}
