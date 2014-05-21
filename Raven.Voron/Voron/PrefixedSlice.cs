// -----------------------------------------------------------------------
//  <copyright file="PrefixedSlice.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Runtime.InteropServices;
using System.Text;
using Voron.Impl;

namespace Voron
{
	[StructLayout(LayoutKind.Explicit, Pack = 1)]
	public struct PrefixedSliceHeader
	{
		[FieldOffset(0)]
		public byte PrefixId;

		[FieldOffset(1)]
		public ushort PrefixUsage;

		[FieldOffset(3)]
		public ushort NonPrefixedDataSize;
	}

	public unsafe class PrefixedSlice
	{
		public const byte NonPrefixed = 0xff;

		private readonly PrefixedSliceHeader* _header;
		private readonly byte* _base;
		private byte* _nonPrefixedData;

		public SliceOptions Options;

		public PrefixedSlice(byte* p)
		{
			_base = p;
			_header = (PrefixedSliceHeader*)_base;
			_nonPrefixedData = _base + Constants.PrefixedSliceHeaderSize;
		}

		public PrefixedSlice(Slice key)
		{
			_base = (byte*)Marshal.AllocHGlobal(Constants.PrefixedSliceHeaderSize + key.Size).ToPointer();
			_header = (PrefixedSliceHeader*)_base;

			_header->PrefixId = NonPrefixed;
			_header->PrefixUsage = 0;
			_header->NonPrefixedDataSize = key.Size;

			_nonPrefixedData = _base + Constants.PrefixedSliceHeaderSize;
			key.CopyTo(_nonPrefixedData);

			Options = key.Options;
		}

		public PrefixedSlice(byte prefixId, ushort prefixUsage, Slice key)
		{
			var nonPrefixedSize = (ushort) (key.Size - prefixUsage);
			_base = (byte*)Marshal.AllocHGlobal(Constants.PrefixedSliceHeaderSize + nonPrefixedSize).ToPointer();
			_header = (PrefixedSliceHeader*)_base;

			_header->PrefixId = prefixId;
			_header->PrefixUsage = prefixUsage;
			_header->NonPrefixedDataSize = nonPrefixedSize;

			_nonPrefixedData = _base + Constants.PrefixedSliceHeaderSize;
			key.CopyTo(prefixUsage, _nonPrefixedData, 0, _header->NonPrefixedDataSize);

			Options = key.Options;
		}

		public ushort Size
		{
			get { return (ushort)(Constants.PrefixedNodeHeaderSize + _header->NonPrefixedDataSize); }
		}

		public byte PrefixId
		{
			get { return _header->PrefixId; }
		}

		public ushort PrefixUsage
		{
			get { return _header->PrefixUsage; }
		}

		public ushort NonPrefixedDataSize
		{
			get { return _header->NonPrefixedDataSize; }
		}

		public byte* NonPrefixedData
		{
			get { return _nonPrefixedData; }
		}

		public void CopyTo(byte* dest)
		{
			NativeMethods.memcpy(dest, _base, Size);
		}

		public override string ToString()
		{
			return string.Format("prefix_id: {0} [usage: {1}], non_prefixed: {2}", _header->PrefixId, _header->PrefixUsage, new string((sbyte*) _nonPrefixedData, 0, _header->NonPrefixedDataSize, Encoding.UTF8));
		}
	}
}