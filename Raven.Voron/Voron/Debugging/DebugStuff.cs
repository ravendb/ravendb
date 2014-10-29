using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using Voron.Impl;
using Voron.Trees;

namespace Voron.Debugging
{
	public class DebugStuff
	{
		private static DateTime _lastGenerated;
        [Conditional("DEBUG")]
        public static void RenderFreeSpace(Transaction tx)
        {
            RenderAndShow(tx, tx.ReadTree(tx.Environment.State.FreeSpaceRoot.Name).State.RootPageNumber, 1);
        }

		[Conditional("DEBUG")]
		public static void DumpHumanReadable(Transaction tx, long startPageNumber,string filenamePrefix = null)
		{
			if (Debugger.IsAttached == false)
				return;
			var path = Path.Combine(Environment.CurrentDirectory, String.Format("{0}tree.hdump", filenamePrefix ?? String.Empty));
			TreeDumper.DumpHumanReadable(tx, path, tx.GetReadOnlyPage(startPageNumber));
		}

		public unsafe static bool HasDuplicateBranchReferences(Transaction tx, Page start,out long pageNumberWithDuplicates)
		{
			var stack = new Stack<Page>();
			var existingTreeReferences = new ConcurrentDictionary<long, List<long>>();
			stack.Push(start);
			while (stack.Count > 0)
			{
				var currentPage = stack.Pop();
				if (currentPage.IsBranch)
				{
					for (int nodeIndex = 0; nodeIndex < currentPage.NumberOfEntries; nodeIndex++)
					{
						var node = currentPage.GetNode(nodeIndex);

						existingTreeReferences.AddOrUpdate(currentPage.PageNumber, new List<long> { node->PageNumber },
							(branchPageNumber, pageNumberReferences) =>
							{
								pageNumberReferences.Add(node->PageNumber);
								return pageNumberReferences;
							});
					}

					for (int nodeIndex = 0; nodeIndex < currentPage.NumberOfEntries; nodeIndex++)
					{
						var node = currentPage.GetNode(nodeIndex);
						if (node->PageNumber < 0 || node->PageNumber > tx.State.NextPageNumber)
						{
							throw new InvalidDataException("found invalid reference on branch - tree is corrupted");
						}

						var child = tx.GetReadOnlyPage(node->PageNumber);
						stack.Push(child);
					}

				}
			}

			Func<long, HashSet<long>> relevantPageReferences =
				branchPageNumber => new HashSet<long>(existingTreeReferences
					.Where(kvp => kvp.Key != branchPageNumber)
					.SelectMany(kvp => kvp.Value));

			// ReSharper disable once LoopCanBeConvertedToQuery
			foreach (var branchReferences in existingTreeReferences)
			{
				if (
					branchReferences.Value.Any(
						referencePageNumber => relevantPageReferences(branchReferences.Key).Contains(referencePageNumber)))
				{
					pageNumberWithDuplicates = branchReferences.Key;
					return true;
				}
			}
			pageNumberWithDuplicates = -1;
			return false;
		}

		[Conditional("DEBUG")]
		public static void RenderAndShow(Transaction tx, long startPageNumber, int showNodesEvery = 25, string format = "svg")
		{
			if (Debugger.IsAttached == false)
				return;

			var dateTime = DateTime.UtcNow;

			if ((dateTime - _lastGenerated).TotalSeconds < 2.5)
			{
				return;
			}
			_lastGenerated = dateTime;

			var path = Path.Combine(Environment.CurrentDirectory, "output.dot");
			TreeDumper.Dump(tx, path, tx.GetReadOnlyPage(startPageNumber), showNodesEvery);

			var output = Path.Combine(Environment.CurrentDirectory, "output." + format);

			var p = Process.Start( FindGraphviz() + @"\bin\dot.exe", "-T" + format + " " + path + " -o " + output);
			p.WaitForExit();
			Process.Start(output);
			Thread.Sleep(500);
		}


	    public static string FindGraphviz()
        {
            var path = @"C:\Program Files (x86)\Graphviz2.";
            for (var i = 0; i < 100; i++)
            {
                var p = path + i.ToString("00");

                if (Directory.Exists(p))
                    return p;
            }

            throw new InvalidOperationException("No Graphviz found.");
        }

	}
}