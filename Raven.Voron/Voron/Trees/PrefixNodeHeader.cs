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
		public readonly PrefixNodeHeader* Header;
		public readonly long PageNumber;
		public readonly byte* ValuePtr;

		public PrefixNode(byte* p, long pageNumber)
		{
			Header = (PrefixNodeHeader*)p;

			PageNumber = pageNumber;
			ValuePtr = (byte*)Header + Constants.PrefixNodeHeaderSize;
		}

		public ushort PrefixLength
		{
			get { return Header->PrefixLength; }
		}
	}
}