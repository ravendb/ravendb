// -----------------------------------------------------------------------
//  <copyright file="PrefixedSlice.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Runtime.InteropServices;
using System.Text;
using Voron.Impl;
using Voron.Trees;

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
		public static PrefixedSlice AfterAllKeys = new PrefixedSlice(SliceOptions.AfterAllKeys);
		public static PrefixedSlice BeforeAllKeys = new PrefixedSlice(SliceOptions.BeforeAllKeys);
		public static PrefixedSlice Empty = new PrefixedSlice();

		public const byte NonPrefixed = 0xff;

		private readonly PrefixedSliceHeader* _header;
		private readonly byte* _base;

		public readonly ushort Size;
		public readonly byte* NonPrefixedData;

		public SliceOptions Options;
		public Slice NewPrefix = null;

		public PrefixedSlice()
		{
			Options = SliceOptions.Key;
			Size = 0;
		}

		public PrefixedSlice(NodeHeader* node)
		{
			_base = (byte*)node + Constants.NodeHeaderSize;
			_header = (PrefixedSliceHeader*)_base;
			
			NonPrefixedData = _base + Constants.PrefixedSliceHeaderSize;
			Options = SliceOptions.Key;
			Size = (ushort)(Constants.PrefixedSliceHeaderSize + _header->NonPrefixedDataSize);
		}

		public PrefixedSlice(Slice key)
		{
			_base = (byte*)Marshal.AllocHGlobal(Constants.PrefixedSliceHeaderSize + key.Size).ToPointer();
			_header = (PrefixedSliceHeader*)_base;

			_header->PrefixId = NonPrefixed;
			_header->PrefixUsage = 0;
			_header->NonPrefixedDataSize = key.Size;

			NonPrefixedData = _base + Constants.PrefixedSliceHeaderSize;
			key.CopyTo(NonPrefixedData);

			Options = key.Options;
			Size = (ushort)(Constants.PrefixedSliceHeaderSize + _header->NonPrefixedDataSize);
		}

		public PrefixedSlice(byte prefixId, ushort prefixUsage, Slice key)
		{
			var nonPrefixedSize = (ushort) (key.Size - prefixUsage);
			_base = (byte*)Marshal.AllocHGlobal(Constants.PrefixedSliceHeaderSize + nonPrefixedSize).ToPointer();
			_header = (PrefixedSliceHeader*)_base;

			_header->PrefixId = prefixId;
			_header->PrefixUsage = prefixUsage;
			_header->NonPrefixedDataSize = nonPrefixedSize;

			NonPrefixedData = _base + Constants.PrefixedSliceHeaderSize;
			key.CopyTo(prefixUsage, NonPrefixedData, 0, _header->NonPrefixedDataSize);

			Options = key.Options;
			Size = (ushort)(Constants.PrefixedSliceHeaderSize + _header->NonPrefixedDataSize);
		}

		public PrefixedSlice(SliceOptions options)
		{
			Options = options;

			_header = null;
			NonPrefixedData = null;
			Size = 0;
		}

		public byte PrefixId
		{
			get { return _header->PrefixId; }
			set { _header->PrefixId = value; }
		}

		public ushort PrefixUsage
		{
			get { return _header->PrefixUsage; }
		}

		public ushort NonPrefixedDataSize
		{
			get { return _header->NonPrefixedDataSize; }
		}

		public void CopyTo(byte* dest)
		{
			NativeMethods.memcpy(dest, _base, Size);
		}

		public override string ToString()
		{
			return string.Format("prefix_id: {0} [usage: {1}], non_prefixed: {2}", _header->PrefixId, _header->PrefixUsage, new string((sbyte*) NonPrefixedData, 0, _header->NonPrefixedDataSize, Encoding.UTF8));
		}
	}
}