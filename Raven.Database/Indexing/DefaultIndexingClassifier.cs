// -----------------------------------------------------------------------
//  <copyright file="FairIndexingSchedulerWithNewIndexesBias.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;

namespace Raven.Database.Indexing
{
	public class DefaultIndexingClassifier : IIndexingClassifier
	{
		private readonly Dictionary<Etag, List<IndexToWorkOn>> empty = new Dictionary<Etag, List<IndexToWorkOn>>();

		public Dictionary<Etag, List<IndexToWorkOn>> GroupMapIndexes(IList<IndexToWorkOn> indexes)
		{
			if (indexes.Count == 0)
				return empty;

			var indexesByIndexedEtag = indexes
                .Where(x => x.Index.IsMapIndexingInProgress == false) // indexes with precomputed docs are processed separately
				.GroupBy(x => x.LastIndexedEtag, RoughEtagEqualityAndComparison.Instance)
				.OrderByDescending(x => x.Key, RoughEtagEqualityAndComparison.Instance)
				.ToList();

			if (indexesByIndexedEtag.Count == 0)
				return empty;

			return indexesByIndexedEtag.ToDictionary(x => x.Min(index => index.LastIndexedEtag), x => x.Select(index => index).ToList());
		}

		// here we compare, but only up to the last 116 bits, not the full 128 bits
		// this means that we can gather documents that are within 4K docs from one another, because
		// at that point, it doesn't matter much, it would be gone within one or two indexing cycles
		public class RoughEtagEqualityAndComparison : IEqualityComparer<Etag>, IComparer<Etag>
		{
			public static RoughEtagEqualityAndComparison Instance = new RoughEtagEqualityAndComparison();

		    public bool Equals(Etag x, Etag y)
		    {
		        return Compare(x, y) == 0;
		    }

		    public int GetHashCode(Etag obj)
		    {
                var start = 0;
                var bytes = obj.ToByteArray();
                for (var i = 0; i < 14; i++)
                {
                    start = (start * 397) ^ bytes[i];
                }
                var last4Bits = bytes[15] >> 4;
                start = (start * 397) ^ last4Bits;
                return start;
		    }

		    public int Compare(Etag x, Etag y)
		    {
               if (x.Restarts == y.Restarts)
               {
                   var delta = Math.Abs(x.Changes - y.Changes);
                   if (delta <= 4096)
                       return 0;

                   return (int)(x.Changes - y.Changes);
               }

		        return (int) (x.Restarts - y.Restarts);
		    }
		}
	}
}