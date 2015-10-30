// -----------------------------------------------------------------------
//  <copyright file="MemorySlice.cs" company="Hibernating Rhinos LTD">
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
        public abstract void Set(TreeNodeHeader* node);

        protected abstract int CompareData(MemorySlice other, ushort size);

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

        public virtual void PrepareForSearching()
        {
        }	
    }
}
