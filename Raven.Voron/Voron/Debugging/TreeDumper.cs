using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Voron.Impl;
using Voron.Impl.Paging;
using Voron.Trees;

namespace Voron.Debugging
{
    public unsafe class TreeDumper
    {
	    public static void DumpHumanReadable(Transaction tx, string path, Page start)
	    {
		    using (var writer = File.CreateText(path))
		    {
                var stack = new Stack<Page>();
                stack.Push(start);
				writer.WriteLine("Root page #{0}",start.PageNumber);
			    while (stack.Count > 0)
			    {
					var currentPage = stack.Pop();
				    if (currentPage.IsLeaf)
				    {						
						writer.WriteLine();
						writer.WriteLine("Page #{0}, NumberOfEntries = {1}, Flags = {2} (Leaf), Used: {3}", currentPage.PageNumber,currentPage.NumberOfEntries,currentPage.Flags, currentPage.SizeUsed);
						if(currentPage.NumberOfEntries <= 0)
							writer.WriteLine("Empty page (tree corrupted?)");
					    
						var key = new Slice(SliceOptions.Key);
					    for (int nodeIndex = 0; nodeIndex < currentPage.NumberOfEntries;nodeIndex++)
					    {
						    var node = currentPage.GetNode(nodeIndex);
							key.Set(node);
							writer.WriteLine("Node #{0}, Flags = {1}, {4} = {2}, Key = {3}, Entry Size: {5}", nodeIndex, node->Flags, node->DataSize, MaxString(key.ToString(), 25), node->Flags == NodeFlags.Data ? "Size" : "Page",
                                SizeOf.NodeEntry(node));
					    }
						writer.WriteLine();
				    }
				    else if(currentPage.IsBranch) 
				    {
						writer.WriteLine();
						writer.WriteLine("Page #{0}, NumberOfEntries = {1}, Flags = {2} (Branch), Used: {3}", currentPage.PageNumber, currentPage.NumberOfEntries, currentPage.Flags, currentPage.SizeUsed);

						var key = new Slice(SliceOptions.Key);
						for (int nodeIndex = 0; nodeIndex < currentPage.NumberOfEntries; nodeIndex++)
						{
							var node = currentPage.GetNode(nodeIndex);
							writer.WriteLine("Node #{2}, {0}  / to page #{1}, Entry Size: {3}", GetBranchNodeString(nodeIndex, key, currentPage, node), node->PageNumber, nodeIndex,
                                SizeOf.NodeEntry(node));
						}

						for (int nodeIndex = 0; nodeIndex < currentPage.NumberOfEntries; nodeIndex++)
						{
							var node = currentPage.GetNode(nodeIndex);
							if (node->PageNumber < 0 || node->PageNumber > tx.State.NextPageNumber)
							{
								writer.Write("Found invalid reference to page #{0}", currentPage.PageNumber);
								stack.Clear();
								break;
							}

							var child = tx.GetReadOnlyPage(node->PageNumber);
							stack.Push(child);
						}
						
						writer.WriteLine();
					}
			    }
		    }
	    }
        public static void Dump(Transaction tx, string path, Page start, int showNodesEvery = 25)
        {
            using (var writer = File.CreateText(path))
            {
                writer.WriteLine(@"
digraph structs {
    node [shape=Mrecord]
    rankdir=LR;
	bgcolor=transparent;
");

                var stack = new Stack<Page>();
                stack.Push(start);
                var references = new StringBuilder();
                while (stack.Count > 0)
                {
                    var p = stack.Pop();

                    writer.WriteLine(@"
	subgraph cluster_p_{0} {{ 
		label=""Page #{0}"";
		color={3};
	p_{0} [label=""Page: {0}|{1}|Entries: {2:#,#} | {4:p} utilization""];

", p.PageNumber, p.Flags, p.NumberOfEntries, p.IsLeaf ? "black" : "blue",
	Math.Round(((AbstractPager.PageSize - p.SizeLeft) / (double)AbstractPager.PageSize), 2));
                    var key = new Slice(SliceOptions.Key);
                    if (p.IsLeaf && showNodesEvery > 0)
                    {
                        writer.WriteLine("		p_{0}_nodes [label=\" Entries:", p.PageNumber);
                        for (int i = 0; i < p.NumberOfEntries; i += showNodesEvery)
                        {
                            if (i != 0 && showNodesEvery >= 5)
                            {
                                writer.WriteLine(" ... {0:#,#} keys redacted ...", showNodesEvery - 1);
                            }
                            var node = p.GetNode(i);
                            key.Set(node);
                            writer.WriteLine("{0} - {2} {1:#,#}", MaxString(key.ToString(), 25),
                                node->DataSize, node->Flags == NodeFlags.Data ? "Size" : "Page");
                        }
                        if (p.NumberOfEntries < showNodesEvery)
                        {
                            writer.WriteLine(" ... {0:#,#} keys redacted ...", p.NumberOfEntries - 1);
                        }
                        writer.WriteLine("\"];");
                    }
                    else if (p.IsBranch)
                    {
                        writer.Write("		p_{0}_refs [label=\"", p.PageNumber);
                        for (int i = 0; i < p.NumberOfEntries; i++)
                        {
                            var node = p.GetNode(i);

                            writer.Write("{3}<{2}> {0}  / to page {1}", GetBranchNodeString(i, key, p, node), node->PageNumber,
                                i, i == 0 ? "" : "|");
                        }
                        writer.WriteLine("\"];");
                        var prev = -1L;
                        for (int i = 0; i < p.NumberOfEntries; i++)
                        {
                            var node = p.GetNode(i);
                            if (node->PageNumber < 0 || node->PageNumber > tx.State.NextPageNumber)
                            {
                                writer.Write("		p_{0}_refs [label=\"CORRUPTED\"; Color=RED];", p.PageNumber);
                                stack.Clear();
                                break;
                            }
                            var child = tx.GetReadOnlyPage(node->PageNumber);
                            stack.Push(child);

                            references.AppendFormat("	p_{0}_refs:{3} -> p_{1} [label=\"{2}\"];", p.PageNumber, child.PageNumber, GetBranchNodeString(i, key, p, node), i).AppendLine();
                            if (prev > -1)
                                references.AppendFormat("	p_{0} -> p_{1} [style=\"invis\"];", child.PageNumber, prev);

                            prev = child.PageNumber;
                        }
                    }
                    writer.WriteLine("	}");
                }
                writer.WriteLine(references.ToString());

                writer.WriteLine("}");
            }
        }

        private static string MaxString(string key, int size)
        {
            if (key.Length <= size)
                return key;
            return key.Substring(0, (size/2)) + "..." + key.Substring(key.Length - size/2, size/2);
        }

        private static unsafe string GetBranchNodeString(int i, Slice key, Page p, NodeHeader* node)
        {
            string keyStr;
            if (i == 0 && key.Size == 0)
            {
                key.Set(p.GetNode(1));
                keyStr = "(lt " + key + ")";
            }
            else
            {
                key.Set(node);
                keyStr = key.ToString();
            }
            return MaxString(keyStr, 25);
        }
    }
}