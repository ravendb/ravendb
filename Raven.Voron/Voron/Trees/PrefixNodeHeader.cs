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
		public byte* Base;
		public PrefixNodeHeader Header;
		public long PageNumber;
		public byte* ValuePtr;
		public byte[] Value;

		public PrefixNode()
		{
			
		}

		public PrefixNode(PrefixNodeHeader header, byte[] value, long pageNumber)
		{
			Header = header;
			Value = value;
			PageNumber = pageNumber;
		}

		public void Set(byte* p, long pageNumber)
		{
			Base = p;
			Header = *((PrefixNodeHeader*)p);
			ValuePtr = p + Constants.PrefixNodeHeaderSize;
			PageNumber = pageNumber;
		}

		public ushort PrefixLength
		{
			get { return Header.PrefixLength; }
		}
	}
}