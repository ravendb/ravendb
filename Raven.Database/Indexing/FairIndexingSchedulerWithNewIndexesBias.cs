// -----------------------------------------------------------------------
//  <copyright file="FairIndexingSchedulerWithNewIndexesBias.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Database.Indexing
{
	public class FairIndexingSchedulerWithNewIndexesBias : IIndexingScheduler
	{
		private int current;
		private int currentRepeated;
		private bool activeFiltering;
		private List<int> lastAmountOfItemsToIndex = new List<int>();
		private List<int> lastAmountOfItemsToReduce = new List<int>();

		public FairIndexingSchedulerWithNewIndexesBias()
		{
			LastAmountOfItemsToIndexToRemember = 1;
			LastAmountOfItemsToReduceToRemember = 1;
		}

		public int LastAmountOfItemsToIndexToRemember { get; set; }

		public int LastAmountOfItemsToReduceToRemember { get; set; }

		public IList<IndexToWorkOn> FilterMapIndexes(IList<IndexToWorkOn> indexes)
		{
			if (indexes.Count == 0)
				return indexes;

			var indexesByIndexedEtag = indexes
				.GroupBy(x => x.LastIndexedEtag, new RoughGuidEqualityAndComparision())
				.OrderBy(x => x.Key, new RoughGuidEqualityAndComparision())
				.ToList();

			if (indexesByIndexedEtag.Count == 1)
			{
				currentRepeated = 0;
				current = 0;
				activeFiltering = false;
				return indexes; // they all have the same one, so there aren't any delayed / new indexes
			}

			activeFiltering = true;

			// we have indexes that haven't all caught up with up yet, so we need to start cycling through the 
			// different levels, starting with the earliest ones, we are biased toward the first ones
			// let us assume that we have 2 levels, which is likely to be the most comomn scenario
			// this would mean that the earliest one would be run twice for every later one run
			if (currentRepeated >= (indexesByIndexedEtag.Count - current))
			{
				current++;
				currentRepeated = 0;
			}

			if (current >= indexesByIndexedEtag.Count) // catch the overflow
				current = 0;

			var indexToWorkOns = indexesByIndexedEtag[current];
			currentRepeated++;

			return indexToWorkOns.ToList();
		}

		public void RecordAmountOfItemsToIndex(int value)
		{
			var currentLastAmountOfItemsToIndex = lastAmountOfItemsToIndex;
			var amountOfItemsToIndex = activeFiltering && currentLastAmountOfItemsToIndex.Count > 0
				// if we are actively filtering, we have multiple levels, so we have to assume 
				// that the max amount is still the current one, this prevent the different levels of indexing batch
				// size from "fighting" over the batch size.
										? Math.Max(currentLastAmountOfItemsToIndex.Max(), value)
										: value;

			var amountToTake = currentLastAmountOfItemsToIndex.Count;
			if (amountToTake + 1 >= LastAmountOfItemsToIndexToRemember)
			{
				amountToTake = currentLastAmountOfItemsToIndex.Count - 1;
			}
			lastAmountOfItemsToIndex = new List<int>(currentLastAmountOfItemsToIndex.Take(amountToTake))
				{
					amountOfItemsToIndex
				};

		}

		public void RecordAmountOfItemsToReduce(int value)
		{
			var currentLastAmountOfItemsToReduce = lastAmountOfItemsToReduce;
			var amountOfItemsToReduce = activeFiltering && currentLastAmountOfItemsToReduce.Count > 0
				// if we are actively filtering, we have multiple levels, so we have to assume 
				// that the max amount is still the current one, this prevent the different levels of indexing batch
				// size from "fighting" over the batch size.
										? Math.Max(currentLastAmountOfItemsToReduce.Max(), value)
										: value;

			var amountToTake = currentLastAmountOfItemsToReduce.Count;
			if (amountToTake + 1 >= LastAmountOfItemsToReduceToRemember)
			{
				amountToTake = currentLastAmountOfItemsToReduce.Count - 1;
			}
			lastAmountOfItemsToReduce = new List<int>(currentLastAmountOfItemsToReduce.Take(amountToTake))
				{
					amountOfItemsToReduce
				};

		}

		public IEnumerable<int> GetLastAmountOfItemsToIndex()
		{
			return lastAmountOfItemsToIndex;
		}

		public IEnumerable<int> GetLastAmountOfItemsToReduce()
		{
			return lastAmountOfItemsToReduce;
		}

		// here we compare, but only up to the last 116 bits, not the full 128 bits
		// this means that we can gather documents that are within 4K docs from one another, because
		// at that point, it doesn't matter much, it would be gone within one or two indexing cycles
		public class RoughGuidEqualityAndComparision : IEqualityComparer<Guid>, IComparer<Guid>
		{
			public bool Equals(Guid x, Guid y)
			{
				return Compare(x, y) == 0;
			}

			public int GetHashCode(Guid obj)
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

			public int Compare(Guid x, Guid y)
			{
				var xBytes = x.ToByteArray();
				var yBytes = y.ToByteArray();

				for (int i = 0; i < 14; i++)
				{
					if (xBytes[i] != yBytes[i])
						return xBytes[i] - yBytes[i];
				}
				var xLast4Bits = xBytes[15] >> 4;
				var yLast4Bits = yBytes[15] >> 4;
				return xLast4Bits - yLast4Bits;
			}
		}
	}
}