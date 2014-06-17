// -----------------------------------------------------------------------
//  <copyright file="PrefixNodeHeader.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Runtime.InteropServices;
using Voron.Impl;

namespace Voron.Trees
{
	[StructLayout(LayoutKind.Explicit, Pack = 1)]
	public struct PrefixNodeHeader
	{
		[FieldOffset(0)]
		public ushort PrefixLength;
	}

	public unsafe class PrefixNode
	{
		public PrefixNodeHeader* Header;
		public long PageNumber;
		public byte* ValuePtr;

		public void Set(byte* p, long pageNumber)
		{
			Header = (PrefixNodeHeader*)p;
			ValuePtr = p + Constants.PrefixNodeHeaderSize;
			PageNumber = pageNumber;
		}

		public ushort PrefixLength
		{
			get { return Header->PrefixLength; }
		}
	}
}