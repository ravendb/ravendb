// -----------------------------------------------------------------------
//  <copyright file="ManagedPrefixSlice.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
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

	public unsafe class PrefixedSlice : MemorySlice
	{
		public static PrefixedSlice AfterAllKeys = new PrefixedSlice(SliceOptions.AfterAllKeys);
		public static PrefixedSlice BeforeAllKeys = new PrefixedSlice(SliceOptions.BeforeAllKeys);
		public static PrefixedSlice Empty = new PrefixedSlice(Slice.Empty)
		{
			Size = 0,
			KeyLength = 0
		};

		public const byte NonPrefixedId = 0xff;

		private readonly PrefixedSliceHeader _header;

		public readonly Slice NonPrefixedData;

		public PrefixNode Prefix;
		public Slice NewPrefix = null;

		public byte* PrefixValue
		{
			get { return Prefix != null ? Prefix.ValuePtr : null; }
		}

		public PrefixedSlice()
		{
			Options = SliceOptions.Key;
			Size = 0;
			KeyLength = 0;
			_header = new PrefixedSliceHeader();
		}

		public PrefixedSlice(SliceOptions options)
			: this()
		{

			Options = options;
		}

		public PrefixedSlice(byte prefixId, ushort prefixUsage, Slice nonPrefixedValue)
		{
			_header = new PrefixedSliceHeader
			{
				PrefixId = prefixId,
				PrefixUsage = prefixUsage,
				NonPrefixedDataSize = nonPrefixedValue.KeyLength
			};

			NonPrefixedData = nonPrefixedValue;
			Size = (ushort)(Constants.PrefixedSliceHeaderSize + nonPrefixedValue.KeyLength);
			KeyLength = (ushort)(prefixUsage + nonPrefixedValue.KeyLength);
			Options = nonPrefixedValue.Options;
		}

		public PrefixedSlice(NodeHeader* node)
		{
			if (node->KeySize > 0)
			{
				var prefixHeaderPtr = (PrefixedSliceHeader*)((byte*)node + Constants.NodeHeaderSize);
				_header = *prefixHeaderPtr;

				NonPrefixedData = new Slice((byte*)prefixHeaderPtr + Constants.PrefixedSliceHeaderSize, _header.NonPrefixedDataSize);

				Size = node->KeySize;
				KeyLength = (ushort) (_header.PrefixUsage + _header.NonPrefixedDataSize);
			}
			else
			{
				Size = 0;
				KeyLength = 0;
			}

			Options = SliceOptions.Key;
		}

		public PrefixedSlice(MemorySlice key)
		{
			_header = new PrefixedSliceHeader
			{
				PrefixId = NonPrefixedId,
				PrefixUsage = 0,
				NonPrefixedDataSize = key.KeyLength
			};

			NonPrefixedData = key.ToSlice();

			Size = (ushort)(Constants.PrefixedSliceHeaderSize + key.KeyLength);
			KeyLength = key.KeyLength;
		}

		public PrefixedSliceHeader Header
		{
			get { return _header; }
		}

		public override void CopyTo(byte* dest)
		{
			var destHeader = (PrefixedSliceHeader*) dest;

			destHeader->PrefixId = _header.PrefixId;
			destHeader->PrefixUsage = _header.PrefixUsage;
			destHeader->NonPrefixedDataSize = _header.NonPrefixedDataSize;

			NonPrefixedData.CopyTo(dest + Constants.PrefixedSliceHeaderSize);
		}

		internal void SetPrefix(PrefixNode prefix)
		{
			Prefix = prefix;
		}

		public override Slice ToSlice()
		{
			return Skip(0);
		}

		public override Slice Skip(ushort bytesToSkip)
		{
			if (_header.PrefixId == NonPrefixedId)
				return NonPrefixedData.Skip(bytesToSkip);

			if (bytesToSkip == _header.PrefixUsage)
				return NonPrefixedData;

			if (bytesToSkip > _header.PrefixUsage)
				return NonPrefixedData.Skip((ushort)(bytesToSkip - _header.PrefixUsage));

			// bytesToSkip < _header.PrefixUsage

			Debug.Assert(Prefix != null);

			var prefixPart = _header.PrefixUsage - bytesToSkip;

			var sliceSize = prefixPart + _header.NonPrefixedDataSize;
			var sliceData = new byte[sliceSize];

			Prefix.Value.CopyTo(bytesToSkip, sliceData, 0, prefixPart);
			NonPrefixedData.CopyTo(0, sliceData, prefixPart, sliceSize - prefixPart);

			return new Slice(sliceData);
		}

		protected override int CompareData(MemorySlice other, SliceComparer cmp, ushort size)
		{
			var prefixedSlice = other as PrefixedSlice;

			if (prefixedSlice != null)
				return SliceComparisonMethods.Compare(this, prefixedSlice, cmp, size);

			var slice = other as Slice;

			if (slice != null)
			{
				return SliceComparisonMethods.Compare(slice, this, cmp, size) * -1;
			}

			throw new NotSupportedException("Cannot compare because of unknown slice type: " + other.GetType());
		}

		public override string ToString()
		{
			if (Prefix != null)
				return new Slice(Prefix.Value, _header.PrefixUsage) + NonPrefixedData.ToString();

			if (_header.PrefixId == NonPrefixedId)
				return NonPrefixedData.ToString();

			return string.Format("prefix_id: {0} [usage: {1}], non_prefixed: {2}", _header.PrefixId, _header.PrefixUsage, NonPrefixedData);
		}
	}
}