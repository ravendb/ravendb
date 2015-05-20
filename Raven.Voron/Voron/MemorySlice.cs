// -----------------------------------------------------------------------
//  <copyright file="AbstractMemorySlice.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using Voron.Impl;
using Voron.Trees;

namespace Voron
{
	public unsafe abstract class MemorySlice
	{
		public ushort Size;
		public ushort KeyLength;
		public SliceOptions Options;

        protected MemorySlice()
        { }

        protected MemorySlice(SliceOptions options)
        {
            this.Options = options;
        }

        protected MemorySlice(SliceOptions options, ushort size)
        {
            this.Options = options;
            this.Size = size;
            this.KeyLength = size;
        }

		protected MemorySlice(SliceOptions options, ushort size, ushort keyLength)
        {
            this.Options = options;
            this.Size = size;
            this.KeyLength = keyLength;
        }


		public abstract void CopyTo(byte* dest);
		public abstract Slice ToSlice();
		public abstract Slice Skip(ushort bytesToSkip);
		public abstract void Set(NodeHeader* node);

		protected abstract int CompareData(MemorySlice other, ushort size);

		protected abstract int CompareData(MemorySlice other, PrefixedSliceComparer cmp, ushort size);

		public bool Equals(MemorySlice other)
		{
			return Compare(other) == 0;
		}

		public int Compare(MemorySlice other)
		{
			Debug.Assert(Options == SliceOptions.Key);
			Debug.Assert(other.Options == SliceOptions.Key);

            var srcKey = this.KeyLength;
            var otherKey = other.KeyLength;
            var length = srcKey <= otherKey ? srcKey : otherKey;

            var r = CompareData(other, length);
			if (r != 0)
				return r;

            return srcKey - otherKey;
		}

		public bool StartsWith(MemorySlice other)
		{
			if (KeyLength < other.KeyLength)
				return false;
			
            return CompareData(other, other.KeyLength) == 0;
		}

		private ushort _matchedBytes;
		private PrefixedSliceComparer _matchPrefixInstance;

		public ushort FindPrefixSize(MemorySlice other)
		{
			_matchedBytes = 0;

			if (_matchPrefixInstance == null)
				_matchPrefixInstance = MatchPrefix;

			CompareData(other, _matchPrefixInstance, Math.Min(KeyLength, other.KeyLength));

			return _matchedBytes;
		}

		private int MatchPrefix(byte* lhs, byte* rhs, int size)
		{
			var n = size;

			var sizeOfUInt = Constants.SizeOfUInt;

			if (n > sizeOfUInt)
			{
				var lUintAlignment = (long)lhs % sizeOfUInt;
				var rUintAlignment = (long)rhs % sizeOfUInt;

				if (lUintAlignment != 0 && lUintAlignment == rUintAlignment)
				{
					var toAlign = sizeOfUInt - lUintAlignment;
					while (toAlign > 0)
					{
						var r = *lhs++ - *rhs++;
						if (r != 0)
							return r;
						n--;
						_matchedBytes++;

						toAlign--;
					}
				}

				uint* lp = (uint*)lhs;
				uint* rp = (uint*)rhs;

				while (n > sizeOfUInt)
				{
					if (*lp != *rp)
						break;

					lp++;
					rp++;

					n -= sizeOfUInt;
					_matchedBytes += sizeOfUInt;
				}

				lhs = (byte*)lp;
				rhs = (byte*)rp;
			}

			while (n > 0)
			{
				var r = *lhs++ - *rhs++;
				if (r != 0)
					return r;
				n--;
				_matchedBytes++;
			}

			return 0;
		}

		public virtual void PrepareForSearching()
		{
		}	
	}
}