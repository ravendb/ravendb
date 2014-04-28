// -----------------------------------------------------------------------
//  <copyright file="Tree.MultiTree.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.IO;
using Voron.Impl;
using Voron.Impl.FileHeaders;
using Voron.Impl.Paging;
using Voron.Util;

namespace Voron.Trees
{
	/* Multi tree behavior
	 * -------------------
	 * A multi tree is a tree that is used only with MultiRead, MultiAdd, MultiDelete
	 * The common use case is a secondary index that allows duplicates. 
	 * 
	 * The API exposed goes like this:
	 * 
	 * MultiAdd("key", "val1"), MultiAdd("key", "val2"), MultiAdd("key", "val3") 
	 * 
	 * And then you can read it back with MultiRead("key") : IIterator
	 * 
	 * When deleting, you delete one value at a time: MultiDelete("key", "val1")
	 * 
	 * The actual values are stored as keys in a separate tree per key. In order to optimize
	 * space usage, multi trees work in the following fashion.
	 * 
	 * If the totale size of the values per key is less than NodeMaxSize, we store them as an embedded
	 * page inside the owning tree. If then are more than the node max size, we create a separate tree
	 * for them and then only store the tree root infromation.
	 */
	public unsafe partial class Tree
	{
		public bool IsMultiValueTree { get; set; }

		public void MultiAdd(Transaction tx, Slice key, Slice value, ushort? version = null)
		{
			if (value == null) throw new ArgumentNullException("value");
			int maxNodeSize = tx.DataPager.MaxNodeSize;
			if (value.Size > maxNodeSize)
				throw new ArgumentException(
					"Cannot add a value to child tree that is over " + maxNodeSize + " bytes in size", "value");
			if (value.Size == 0)
				throw new ArgumentException("Cannot add empty value to child tree");

			State.IsModified = true;

			Lazy<Cursor> lazy;
			var page = FindPageFor(tx, key, out lazy);
			if ((page == null || page.LastMatch != 0))
			{
				MultiAddOnNewValue(tx, key, value, version, maxNodeSize);
				return;
			}

			page = tx.ModifyPage(page.PageNumber, page);

			var item = page.GetNode(page.LastSearchPosition);

			// already was turned into a multi tree, not much to do here
			if (item->Flags == NodeFlags.MultiValuePageRef)
			{
				var existingTree = OpenOrCreateMultiValueTree(tx, key, item);
				existingTree.DirectAdd(tx, value, 0, version: version);
				return;
			}

			var nestedPagePtr = NodeHeader.DirectAccess(tx, item);
			var nestedPage = new Page(nestedPagePtr, "multi tree", (ushort) NodeHeader.GetDataSize(tx, item));

			var existingItem = nestedPage.Search(value, NativeMethods.memcmp);
			if (nestedPage.LastMatch != 0)
				existingItem = null;// not an actual match, just greater than

			ushort previousNodeRevision = existingItem != null ?  existingItem->Version : (ushort)0;
			CheckConcurrency(key, value, version, previousNodeRevision, TreeActionType.Add);
			
			if (existingItem != null)
			{
				// maybe same value added twice?
				var tmpKey = new Slice(item);
				if (tmpKey.Compare(value, _cmp) == 0)
					return; // already there, turning into a no-op
				nestedPage.RemoveNode(nestedPage.LastSearchPosition);
			}

			if (nestedPage.HasSpaceFor(tx, value, 0))
			{
				// we are now working on top of the modified root page, we can just modify the memory directly
				nestedPage.AddDataNode(nestedPage.LastSearchPosition, value, 0, previousNodeRevision);
				return;
			}

			int pageSize = nestedPage.CalcSizeUsed() + Constants.PageHeaderSize;
			var newRequiredSize = pageSize + nestedPage.GetRequiredSpace(value, 0);
			if (newRequiredSize <= maxNodeSize)
			{
				// we can just expand the current value... no need to create a nested tree yet
				var actualPageSize = (ushort)Math.Min(Utils.NearestPowerOfTwo(newRequiredSize), maxNodeSize);
				ExpandMultiTreeNestedPageSize(tx, key, value, nestedPagePtr, actualPageSize, item->DataSize);

				return;
			}
			// we now have to convert this into a tree instance, instead of just a nested page
			var tree = Create(tx, _cmp, TreeFlags.MultiValue);
			for (int i = 0; i < nestedPage.NumberOfEntries; i++)
			{
				var existingValue = nestedPage.GetNodeKey(i);
				tree.DirectAdd(tx, existingValue, 0);
			}
			tree.DirectAdd(tx, value, 0, version: version);
			tx.AddMultiValueTree(this, key, tree);
			// we need to record that we switched to tree mode here, so the next call wouldn't also try to create the tree again
			DirectAdd(tx, key, sizeof (TreeRootHeader), NodeFlags.MultiValuePageRef);
		}

		private void ExpandMultiTreeNestedPageSize(Transaction tx, Slice key, Slice value, byte* nestedPagePtr, ushort newSize, int currentSize)
		{
			Debug.Assert(newSize > currentSize);
			TemporaryPage tmp;
			using (tx.Environment.GetTemporaryPage(tx, out tmp))
			{
				var tempPagePointer = tmp.TempPagePointer;
				NativeMethods.memcpy(tempPagePointer, nestedPagePtr, currentSize);
				Delete(tx, key); // release our current page
				Page nestedPage = new Page(tempPagePointer, "multi tree", (ushort)currentSize);

				var ptr = DirectAdd(tx, key, newSize);

				var newNestedPage = new Page(ptr, "multi tree", newSize)
				{
					Lower = (ushort)Constants.PageHeaderSize,
					Upper = newSize,
					Flags = PageFlags.Leaf,
					PageNumber = -1L // mark as invalid page number
				};

				Slice nodeKey = new Slice(SliceOptions.Key);
				for (int i = 0; i < nestedPage.NumberOfEntries; i++)
				{
					var nodeHeader = nestedPage.GetNode(i);
					nodeKey.Set(nodeHeader);
					newNestedPage.AddDataNode(i, nodeKey, 0,
						(ushort)(nodeHeader->Version - 1)); // we dec by one because AdddataNode will inc by one, and we don't want to change those values
				}

				newNestedPage.Search(key, _cmp);
				newNestedPage.AddDataNode(newNestedPage.LastSearchPosition, value, 0, 0);
			}
		}

		private void MultiAddOnNewValue(Transaction tx, Slice key, Slice value, ushort? version, int maxNodeSize)
		{
			var requiredPageSize = Constants.PageHeaderSize + Constants.NodeHeaderSize + Constants.NodeOffsetSize + value.Size;
			if (requiredPageSize > maxNodeSize)
			{
				// no choice, very big value, we might as well just put it in its own tree from the get go...
				// otherwise, we would have to put this in overflow page, and that won't save us any space anyway

				var tree = Create(tx, _cmp, TreeFlags.MultiValue);
				tree.DirectAdd(tx, value, 0);
				tx.AddMultiValueTree(this, key, tree);

				DirectAdd(tx, key, sizeof (TreeRootHeader), NodeFlags.MultiValuePageRef);
				return;
			}

			var actualPageSize = (ushort) Math.Min(Utils.NearestPowerOfTwo(requiredPageSize), maxNodeSize);

			var ptr = DirectAdd(tx, key, actualPageSize);

			var nestedPage = new Page(ptr, "multi tree", actualPageSize)
			{
				PageNumber = -1L,// hint that this is an inner page
				Lower = (ushort) Constants.PageHeaderSize,
				Upper = actualPageSize,
				Flags = PageFlags.Leaf,
			};

			CheckConcurrency(key, value, version, 0, TreeActionType.Add);

			nestedPage.AddDataNode(0, value, 0, 0);
		}

		public void MultiDelete(Transaction tx, Slice key, Slice value, ushort? version = null)
		{
			State.IsModified = true;
			Lazy<Cursor> lazy;
			var page = FindPageFor(tx, key, out lazy);
			if (page == null || page.LastMatch != 0)
			{
				return; //nothing to delete - key not found
			}

			page = tx.ModifyPage(page.PageNumber, page);

			var item = page.GetNode(page.LastSearchPosition);

			if (item->Flags == NodeFlags.MultiValuePageRef) //multi-value tree exists
			{
				var tree = OpenOrCreateMultiValueTree(tx, key, item);

				tree.Delete(tx, value, version);

				// previously, we would convert back to a simple model if we dropped to a single entry
				// however, it doesn't really make sense, once you got enough values to go to an actual nested 
				// tree, you are probably going to remain that way, or be removed completely.
				if (tree.State.EntriesCount != 0) 
					return;
				tx.TryRemoveMultiValueTree(this, key);
				tx.FreePage(tree.State.RootPageNumber);
				Delete(tx, key);
			}
			else // we use a nested page here
			{
				var nestedPage = new Page(NodeHeader.DirectAccess(tx, item), "multi tree", (ushort)NodeHeader.GetDataSize(tx, item));
				var nestedItem = nestedPage.Search(value, NativeMethods.memcmp);
				if (nestedItem == null) // value not found
					return;

				CheckConcurrency(key, value, version, nestedItem->Version, TreeActionType.Delete);
				nestedPage.RemoveNode(nestedPage.LastSearchPosition);
				if (nestedPage.NumberOfEntries == 0)
					Delete(tx, key);
			}
		}

		public IIterator MultiRead(Transaction tx, Slice key)
		{
			Lazy<Cursor> lazy;
			var page = FindPageFor(tx, key, out lazy);

			if (page == null || page.LastMatch != 0)
			{
				return new EmptyIterator();
			}

			var item = page.Search(key, _cmp);

			var fetchedNodeKey = new Slice(item);
			if (fetchedNodeKey.Compare(key, _cmp) != 0)
			{
				throw new InvalidDataException("Was unable to retrieve the correct node. Data corruption possible");
			}

			if (item->Flags == NodeFlags.MultiValuePageRef)
			{
				var tree = OpenOrCreateMultiValueTree(tx, key, item);

				return tree.Iterate(tx);
			}

			var nestedPage = new Page(NodeHeader.DirectAccess(tx, item), "multi tree", (ushort)NodeHeader.GetDataSize(tx, item));
				
			return new PageIterator(_cmp, nestedPage);
		}

		internal Tree OpenOrCreateMultiValueTree(Transaction tx, Slice key, NodeHeader* item)
		{
			Tree tree;
			if (tx.TryGetMultiValueTree(this, key, out tree))
				return tree;

			var childTreeHeader =
				(TreeRootHeader*)((byte*)item + item->KeySize + Constants.NodeHeaderSize);
			Debug.Assert(childTreeHeader->RootPageNumber < tx.State.NextPageNumber);
			tree = childTreeHeader != null ?
				Open(tx, _cmp, childTreeHeader) :
				Create(tx, _cmp);

			tx.AddMultiValueTree(this, key, tree);
			return tree;
		}

		public bool SetAsMultiValueTreeRef(Transaction tx, Slice key)
		{
			Lazy<Cursor> lazy;
			var foundPage = FindPageFor(tx, key, out lazy);
			var page = tx.ModifyPage(foundPage.PageNumber, foundPage);

			if (page.LastMatch != 0)
				return false; // not there

			var nodeHeader = page.GetNode(page.LastSearchPosition);
			if (nodeHeader->Flags == NodeFlags.MultiValuePageRef)
				return false;
			if (nodeHeader->Flags != NodeFlags.Data)
				throw new InvalidOperationException("Only data nodes can be set to MultiValuePageRef");
			nodeHeader->Flags = NodeFlags.MultiValuePageRef;
			return true;
		}

		private bool TryOverwriteDataOrMultiValuePageRefNode(NodeHeader* updatedNode, Slice key, int len,
														NodeFlags requestedNodeType, ushort? version,
														out byte* pos)
		{
			switch (requestedNodeType)
			{
				case NodeFlags.Data:
				case NodeFlags.MultiValuePageRef:
					{
						if (updatedNode->DataSize == len &&
							(updatedNode->Flags == NodeFlags.Data || updatedNode->Flags == NodeFlags.MultiValuePageRef))
						{
							CheckConcurrency(key, version, updatedNode->Version, TreeActionType.Add);

							if (updatedNode->Version == ushort.MaxValue)
								updatedNode->Version = 0;
							updatedNode->Version++;

							updatedNode->Flags = requestedNodeType;

							{
								pos = (byte*)updatedNode + Constants.NodeHeaderSize + key.Size;
								return true;
							}
						}
						break;
					}
				case NodeFlags.PageRef:
					throw new InvalidOperationException("We never add PageRef explicitly");
				default:
					throw new ArgumentOutOfRangeException();
			}
			pos = null;
			return false;
		}
	}
}