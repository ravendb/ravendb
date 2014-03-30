// -----------------------------------------------------------------------
//  <copyright file="SkipList.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Voron.Util
{
	using System;
	using System.Diagnostics;
	using System.Threading;

	using System.Collections.Generic;
	using System.Runtime.CompilerServices;

	/// <summary>
	///  Thread safety
	///  -------------
	/// 
	///  Writes require external synchronization, most likely a mutex.
	///  Reads require a guarantee that the SkipList will not be destroyed
	///  while the read is in progress.  Apart from that, reads progress
	///  without any internal locking or synchronization.
	/// </summary>
	public class SkipList<TKey, TVal>
	{
		private readonly IComparer<TKey> _comparer;
		public const int SkipListMaxHeight = 17;

		private readonly Node head = new Node(default(TKey), default(TVal), SkipListMaxHeight);

		// Modified only by Insert, allowed to be read racily by readers
		private int _maxHeight = 1;

		// Only used by Insert, which require externa syncronizastion
		private Random _rnd = new Random();

		public int MaxHeight
		{
			get { return Volatile.Read(ref _maxHeight); }
		}

		public int Count { get; private set; }

		public SkipList(IComparer<TKey> comparer)
		{
			_comparer = comparer;
		}

		public bool Contains(TKey key)
		{
			var x = FindGreaterOrEqual(key, null);
			return (x != null && Equal(key, x.Key));
		}

		public bool Remove(TKey key, out Node node)
		{
			Node current = head;
            node = null;

			bool found = false;
			for (var i = MaxHeight - 1; i >= 0; i--)
			{
				for (; current.Next(i) != null; current = current.Next(i))
				{
					var result = _comparer.Compare(current.Next(i).Key, key);
					if (result == 0)
					{
						found = true;
					    node = current.Next(i);
						current.SetNext(i, current.Next(i).Next(i));

						break;
					}

					if (result > 0)
						break;
				}
			}

			if (found)
				Count--;

			return found;
		}

		public bool Insert(TKey key, TVal val, out Node node)
		{
			var prev = new Node[SkipListMaxHeight];
			Node x = FindGreaterOrEqual(key, prev);

			if (x != null && Equal(key, x.Key))
			{
				x.Val = val;
				node = x;
				return false;
			}

			int height = RandomHeight();
			if (height > MaxHeight)
			{
				for (int i = MaxHeight; i < height; i++)
				{
					prev[i] = head;
				}

				// It is ok to mutate max_height_ without any synchronization
				// with concurrent readers.  A concurrent reader that observes
				// the new value of max_height_ will see either the old value of
				// new level pointers from head_ (NULL), or a new value set in
				// the loop below.  In the former case the reader will
				// immediately drop to the next level since NULL sorts after all
				// keys.  In the latter case the reader will use the new node.
				_maxHeight = height;
			}

			x = new Node(key, val, height);
			for (int i = 0; i < height; i++)
			{
				// NoBarrier_SetNext() suffices since we will add a barrier when
				// we publish a pointer to "x" in prev[i].
				x.SetNextWithNoBarrier(i, prev[i].GetNextWithNoBarrier(i));
				prev[i].SetNext(i, x);
			}

			Count++;
			node = x;
			return true;
		}

		public Node FindGreaterOrEqual(TKey key, Node[] prev)
		{
			Node x = head;
			int level = MaxHeight - 1;
			while (true)
			{
				var next = x.Next(level);
				if ((next != null) && (_comparer.Compare(next.Key, key) < 0)) // KeyIsAfterNode inline for performance
				{
					// Keep searching in this list
					x = next;
				}
				else
				{
					if (prev != null)
						prev[level] = x;
					if (level == 0)
					{
						return next;
					}
					// Switch to next list
					level--;
				}
			}
		}

		private Node FindLessThan(TKey key)
		{
			Node x = head;
			int level = MaxHeight - 1;
			while (true)
			{
				Debug.Assert(x == head || _comparer.Compare(x.Key, key) < 0);
				Node next = x.Next(level);
				if (next == null || _comparer.Compare(next.Key, key) >= 0)
				{
					if (level == 0)
					{
						return x;
					}
					// Switch to next list
					level--;
				}
				else
				{
					x = next;
				}
			}
		}

		private Node FindLast()
		{
			Node x = head;
			int level = MaxHeight - 1;
			while (true)
			{
				Node next = x.Next(level);
				if (next == null)
				{
					if (level == 0)
					{
						return x;
					}
					// Switch to next list
					level--;
				}
				else
				{
					x = next;
				}
			}
		}

		private bool Equal(TKey a, TKey b)
		{
			return _comparer.Compare(a, b) == 0;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		private bool KeyIsAfterNode(TKey key, Node n)
		{
			// NULL n is considered infinite
			return (n != null) && (_comparer.Compare(n.Key, key) < 0);
		}

		private int RandomHeight()
		{
			// Increase height with probability 1 in kBranching
			const int branching = 4;
			int height = 1;
			while (height < SkipListMaxHeight && ((_rnd.Next()%branching) == 0))
			{
				height++;
			}
			Debug.Assert(height > 0);
			Debug.Assert(height <= SkipListMaxHeight);
			return height;
		}


		public class Node
		{
			public TVal Val;
			public TKey Key;
			private readonly Node[] next;

			public Node(TKey key, TVal val, int height)
			{
				Val = val;
				Key = key;
				next = new Node[height];
			}

			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			public Node Next(int i)
			{
				return Volatile.Read(ref next[i]);
			}

			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			public void SetNext(int i, Node val)
			{
				next[i] = val; // write in C# are volatile anyway
			}

			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			public Node GetNextWithNoBarrier(int i)
			{
				return next[i];
			}

			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			public void SetNextWithNoBarrier(int i, Node val)
			{
				next[i] = val;
			}
		}
	}

	public class SkipList<TKey> : SkipList<TKey, TKey>
	{
		public SkipList(IComparer<TKey> comparer)
			: base(comparer)
		{
		}

		public bool Insert(TKey key, out Node node)
		{
			return Insert(key, key, out node);
		}
	}
}

