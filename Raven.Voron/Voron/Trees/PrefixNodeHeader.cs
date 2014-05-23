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
		public readonly int Size;

		public PrefixNode(byte* p)
		{
			_header = (PrefixNodeHeader*)p;

			Base = p;
			Value = new Slice((byte*)_header + Constants.PrefixNodeHeaderSize, _header->PrefixLength);
			Size = Constants.PrefixNodeHeaderSize + _header->PrefixLength;
		}

		public ushort PrefixLength
		{
			get { return _header->PrefixLength; }
		}

		public Slice Value { get; private set; }
	}
}