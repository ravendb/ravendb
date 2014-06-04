// -----------------------------------------------------------------------
//  <copyright file="IMemorySlice.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Voron
{
	public unsafe interface IMemorySlice
	{
		ushort Size { get; }
		ushort KeyLength { get; }
		SliceOptions Options { get; }

		bool Equals(IMemorySlice other);
		int Compare(IMemorySlice other);
		bool StartsWith(IMemorySlice other);
		ushort FindPrefixSize(IMemorySlice other);
		void CopyTo(byte* dest);
		void CopyTo(byte[] dest);

		Slice Skip(ushort bytesToSkip);

		ValueReader CreateReader();
	}
}