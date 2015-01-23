using System;
using System.Collections.Generic;

namespace Voron.Trees
{
	public unsafe class EmptyIterator : IIterator
	{
		public bool Seek(Slice key)
		{
			return false;
		}

		public Slice CurrentKey
		{
			get { throw new InvalidOperationException("No current page"); }
		}

		public int GetCurrentDataSize()
		{
			throw new InvalidOperationException("No current page");
		}

		public bool Skip(int count)
		{
			throw new InvalidOperationException("No records");
		}

		public ValueReader CreateReaderForCurrent()
		{
			throw new InvalidOperationException("No current page");
		}

		public T ReadStructForCurrent<T>() where T : struct
		{
			throw new InvalidOperationException("No current page");
		}

		public IEnumerable<string> DumpValues()
		{
			yield break;
		}

		public unsafe NodeHeader* Current
		{
			get
			{
				throw new InvalidOperationException("No current page");
			}
		}

		public Slice MaxKey { get; set; }

		public Slice RequiredPrefix
		{
			get;
			set;
		}

		public bool MoveNext()
		{
			return false;
		}

		public bool MovePrev()
		{
			return false;
		}

		public void Dispose()
		{
		}
	}
}