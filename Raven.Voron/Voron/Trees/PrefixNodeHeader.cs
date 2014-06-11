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
		private readonly PrefixNodeHeader* _header;
		public readonly byte* Base;
		public readonly long PageNumber;
		public readonly int Size;
		public readonly byte* ValuePtr;
		public readonly Slice Value;


		public PrefixNode(byte* p, long pageNumber)
		{
			_header = (PrefixNodeHeader*)p;

			Base = p;
			PageNumber = pageNumber;
			ValuePtr = (byte*)_header + Constants.PrefixNodeHeaderSize;
			Value = new Slice(ValuePtr, _header->PrefixLength);
			Size = Constants.PrefixNodeHeaderSize + _header->PrefixLength;
		}

		public ushort PrefixLength
		{
			get { return _header->PrefixLength; }
		}
	}
}