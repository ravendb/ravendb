// -----------------------------------------------------------------------
//  <copyright file="StorageReportGenerator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Voron.Impl;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Trees;
using Voron.Trees.Fixed;

namespace Voron.Debugging
{
	public class ReportInput
	{
		public long NumberOfAllocatedPages;
		public long NumberOfFreePages;
		public long NextPageNumber;
		public List<Tree> Trees;
		public List<JournalFile> Journals;
		public bool IsLightReport { get; set; }
	}

	public class StorageReportGenerator
	{
		public class TreeDensityInput
		{
			public readonly List<double> Densities = new List<double>();
		}

		private readonly Transaction _tx;

		public StorageReportGenerator(Transaction tx)
		{
			_tx = tx;
		}

		public StorageReport Generate(ReportInput input)
		{
			var unallocatedPagesAtEndOfFile = input.NumberOfAllocatedPages - input.NextPageNumber;

			var dataFile = new DataFileReport
			{
				AllocatedSpaceInBytes = PagesToBytes(input.NumberOfAllocatedPages),
				SpaceInUseInBytes = PagesToBytes(input.NextPageNumber - input.NumberOfFreePages),
				FreeSpaceInBytes = PagesToBytes(input.NumberOfFreePages + unallocatedPagesAtEndOfFile)
			};

			var trees = new List<TreeReport>();

			foreach (var tree in input.Trees)
			{
				List<double> densities = null;
				if (!input.IsLightReport)
				{
					densities = new List<double>();				
					var allPages = tree.AllPages();

					for (var i = 0; i < allPages.Count; i++)
					{
						var page = _tx.GetReadOnlyPage(allPages[i]);

						if (page.IsOverflow)
						{
							var numberOfPages = _tx.DataPager.GetNumberOfOverflowPages(page.OverflowSize);

							densities.Add(((double) (page.OverflowSize + Constants.PageHeaderSize))/(numberOfPages*AbstractPager.PageSize));

							i += (numberOfPages - 1);
						}
						else
						{
							if (page.IsFixedSize)
							{
								var sizeUsed = Constants.PageHeaderSize + (page.FixedSize_NumberOfEntries*(page.IsLeaf ? page.FixedSize_ValueSize : FixedSizeTree.BranchEntrySize));
								densities.Add(((double) sizeUsed)/AbstractPager.PageSize);
							}
							else
							{
								densities.Add(((double) page.SizeUsed)/AbstractPager.PageSize);
							}
						}
					}
				}
				var state = tree.State;
				var treeReport = new TreeReport
				{
					Name = tree.Name,
					BranchPages = state.BranchPages,
					Depth = state.Depth,
					EntriesCount = state.EntriesCount,
					LeafPages = state.LeafPages,
					OverflowPages = state.OverflowPages,
					PageCount = state.PageCount,
					Density = input.IsLightReport?0:CalculateTreeDensity(densities)
				};

				trees.Add(treeReport);
			}

			var journals = input.Journals.Select(journal => new JournalReport
			{
				Number = journal.Number,
				AllocatedSpaceInBytes = PagesToBytes(journal.JournalWriter.NumberOfAllocatedPages)
			}).ToList();

			return new StorageReport
			{
				DataFile = dataFile,
				Trees = trees,
				Journals = journals
			};
		}

		private static long PagesToBytes(long pageCount)
		{
			return pageCount * AbstractPager.PageSize;
		}

		public static double CalculateTreeDensity(List<double> pageDensities)
		{
			return pageDensities.Sum(x => x) / pageDensities.Count;
		}
	}
}