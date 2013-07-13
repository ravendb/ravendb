using System;
using System.Data;
using System.IO;

namespace Nevar
{
	public unsafe class Tree
	{
		public int BranchPages;
		public int LeafPages;
		public int OverflowPages;
		public int Depth;
		public bool Dirty;

		private readonly SliceComparer _cmp;
		private readonly Page _root;

		private Tree(SliceComparer cmp, Page root)
		{
			_cmp = cmp;
			_root = root;
		}

		public static Tree CreateOrOpen(Transaction tx, int root, SliceComparer cmp)
		{
			if (root != -1)
			{
				return new Tree(cmp, tx.GetPage(root));
			}

			// need to create the root
			var newRootPage = NewPage(tx, PageFlags.Leaf, 1);
			var tree = new Tree(cmp, newRootPage);
			tree.RecordNewPage(newRootPage, 1);
			tree.Depth++;
			tree.Dirty = true;

			return tree;
		}

		public void Add(Transaction tx, Slice key, Stream value)
		{
			if (value == null) throw new ArgumentNullException("value");
			if(value.Length > int.MaxValue) throw new ArgumentException("Cannot add a value that is over 2GB in size", "value");

			var page = FindPageFor(tx, key);

			page.AddNode(page.LastSearchPosition, key, value, 0);
		}

		public Page FindPageFor(Transaction tx, Slice key)
		{
			var p = _root;
			while (p.Header->Flags.HasFlag(PageFlags.Branch))
			{
				ushort nodePos;
				if (key.Options == SliceOptions.BeforeAllKeys)
				{
					nodePos = 0;
				}
				else if (key.Options == SliceOptions.AfterAllKeys)
				{
					nodePos = (ushort) (p.NumberOfEntries - 1);
				}
				else
				{
					bool exactMatch;
					if (p.Search(key, _cmp, out exactMatch) == null)
					{
						nodePos = (ushort) (p.NumberOfEntries - 1);
					}
					else
					{
						nodePos = p.LastSearchPosition;
						if (!exactMatch)
							nodePos--;

					}
				}

				var node = p.GetNode(nodePos);
				p = tx.GetPage(node->PageNumber);
			}

			if (p.IsLeaf == false)
				throw new DataException("Index points to a non leaf page");

			return p;
		}

		private void RecordNewPage(Page p, int num)
		{
			var flags = p.Header->Flags;
			if (flags.HasFlag(PageFlags.Branch))
			{
				BranchPages++;
			}
			else if (flags.HasFlag(PageFlags.Leaf))
			{
				LeafPages++;
			}
			else if (flags.HasFlag(PageFlags.Overlfow))
			{
				OverflowPages += num;
				p.Header->NumberOfPages = num;
			}
		}

		private static Page NewPage(Transaction tx, PageFlags flags, int num)
		{
			var page = tx.AllocatePage(num);

			page.Header->Flags = flags | PageFlags.Dirty;

			page.Header->Lower = (ushort)sizeof(PageHeader);
			page.Header->Upper = Constants.PageSize;

			return page;
		}
	}
}