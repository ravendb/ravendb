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

namespace Voron.Trees
{
	public unsafe partial class Tree
	{
		public bool IsMultiValueTree { get; set; }

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

				if (tree.State.EntriesCount > 1)
					return;

				if (tree.State.EntriesCount == 0)
				{
					tx.TryRemoveMultiValueTree(this, key);
					tx.FreePage(tree.State.RootPageNumber);
					return;
				}

				// convert back to simple key/val
				var iterator = tree.Iterate(tx);
				if (!iterator.Seek(Slice.BeforeAllKeys))
					throw new InvalidDataException(
						"MultiDelete() failed : sub-tree is empty where it should not be, this is probably a Voron bug.");

				var dataToSave = iterator.CurrentKey;

				if (iterator.Current->DataSize != 0)
					return; // we can't move this to a key/value, it has a stream value

				var ptr = DirectAdd(tx, key, dataToSave.Size);
				dataToSave.CopyTo(ptr);

				tx.TryRemoveMultiValueTree(this, key);
				tx.FreePage(tree.State.RootPageNumber);
			}
			else //the regular key->value pattern
			{
				Delete(tx, key, version);
			}
		}

		public void MultiAdd(Transaction tx, Slice key, Slice value, ushort? version = null)
		{
			if (value == null) throw new ArgumentNullException("value");
			if (value.Size > tx.DataPager.MaxNodeSize)
				throw new ArgumentException(
					"Cannot add a value to child tree that is over " + tx.DataPager.MaxNodeSize + " bytes in size", "value");
			if (value.Size == 0)
				throw new ArgumentException("Cannot add empty value to child tree");

			State.IsModified = true;

			Lazy<Cursor> lazy;
			var page = FindPageFor(tx, key, out lazy);
			if ((page == null || page.LastMatch != 0))
			{
				var ptr = DirectAdd(tx, key, value.Size, version: version);
				value.CopyTo(ptr);
				return;
			}

			page = tx.ModifyPage(page.PageNumber, page);

			var item = page.GetNode(page.LastSearchPosition);

			CheckConcurrency(key, version, item->Version, TreeActionType.Add);
			var existingValue = new Slice(DirectRead(tx, key), (ushort)item->DataSize);
			if (existingValue.Compare(value, _cmp) == 0)
				return; //nothing to do, the exact value is already there				


			if (item->Flags == NodeFlags.MultiValuePageRef)
			{
				var tree = OpenOrCreateMultiValueTree(tx, key, item);
				tree.DirectAdd(tx, value, 0);
			}
			else // need to turn to tree
			{
				var tree = Create(tx, _cmp, TreeFlags.MultiValue);
				var current = NodeHeader.GetData(tx, item);
				tree.DirectAdd(tx, current, 0);
				tree.DirectAdd(tx, value, 0);
				tx.AddMultiValueTree(this, key, tree);

				// we need to record that we switched to tree mode here, so the next call wouldn't also try to create the tree again
				DirectAdd(tx, key, sizeof(TreeRootHeader), NodeFlags.MultiValuePageRef);
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

			return new SingleEntryIterator(_cmp, item, tx);
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