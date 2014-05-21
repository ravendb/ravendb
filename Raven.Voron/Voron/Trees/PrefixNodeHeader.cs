// -----------------------------------------------------------------------
//  <copyright file="PrefixNodeHeader.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Runtime.InteropServices;

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

		public PrefixNode(byte* p)
		{
			_header = (PrefixNodeHeader*) p;
			Value = new Slice((byte*)_header + sizeof(PrefixNodeHeader), _header->PrefixLength);
		}

		public Slice Value { get; private set; }
	}
}