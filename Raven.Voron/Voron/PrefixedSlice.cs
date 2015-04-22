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
	using Util;

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

	public unsafe sealed class PrefixedSlice : MemorySlice
	{
		public static PrefixedSlice AfterAllKeys = new PrefixedSlice(SliceOptions.AfterAllKeys);
		public static PrefixedSlice BeforeAllKeys = new PrefixedSlice(SliceOptions.BeforeAllKeys);
		public static PrefixedSlice Empty = new PrefixedSlice(Slice.Empty)
		{
			Size = 0,
			KeyLength = 0
		};

		public const byte NonPrefixedId = 0xff;

		public readonly Slice NonPrefixedData;

		public PrefixedSliceHeader Header;
		public PrefixNode Prefix;
		public Slice NewPrefix;

		public PrefixedSlice()
		{
			Options = SliceOptions.Key;
			Size = 0;
			KeyLength = 0;
			Header = new PrefixedSliceHeader();
			NonPrefixedData = new Slice(SliceOptions.Key);
		}

		public PrefixedSlice(SliceOptions options)
			: this()
		{

			Options = options;
		}

		public PrefixedSlice(byte prefixId, ushort prefixUsage, Slice nonPrefixedValue)
		{
			Header = new PrefixedSliceHeader
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
				Header = *prefixHeaderPtr;

				NonPrefixedData = new Slice((byte*)prefixHeaderPtr + Constants.PrefixedSliceHeaderSize, Header.NonPrefixedDataSize);

				Size = node->KeySize;
				KeyLength = (ushort) (Header.PrefixUsage + Header.NonPrefixedDataSize);
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
			Header = new PrefixedSliceHeader
			{
				PrefixId = NonPrefixedId,
				PrefixUsage = 0,
				NonPrefixedDataSize = key.KeyLength
			};

			NonPrefixedData = key.ToSlice();

			Size = (ushort)(Constants.PrefixedSliceHeaderSize + key.KeyLength);
			KeyLength = key.KeyLength;
			Options = SliceOptions.Key;
		}

		public override void Set(NodeHeader* node)
		{
			Debug.Assert(this != Empty, "Cannot call Set() on PrefixedSlice.Empty");

			if (node->KeySize > 0)
			{
				var prefixHeaderPtr = (PrefixedSliceHeader*)((byte*)node + Constants.NodeHeaderSize);
				Header = *prefixHeaderPtr;

				NonPrefixedData.Set((byte*)prefixHeaderPtr + Constants.PrefixedSliceHeaderSize, Header.NonPrefixedDataSize);

				Size = node->KeySize;
				KeyLength = (ushort)(Header.PrefixUsage + Header.NonPrefixedDataSize);
			}
			else
			{
				Size = 0;
				KeyLength = 0;
			}
		}

		public override void CopyTo(byte* dest)
		{
			var destHeader = (PrefixedSliceHeader*) dest;

			destHeader->PrefixId = Header.PrefixId;
			destHeader->PrefixUsage = Header.PrefixUsage;
			destHeader->NonPrefixedDataSize = Header.NonPrefixedDataSize;

			NonPrefixedData.CopyTo(dest + Constants.PrefixedSliceHeaderSize);
		}

		public override Slice ToSlice()
		{
			return Skip(0);
		}

		public override Slice Skip(ushort bytesToSkip)
		{
			if (Header.PrefixId == NonPrefixedId)
				return NonPrefixedData.Skip(bytesToSkip);

			if (bytesToSkip == Header.PrefixUsage)
				return NonPrefixedData;

			if (bytesToSkip > Header.PrefixUsage)
				return NonPrefixedData.Skip((ushort)(bytesToSkip - Header.PrefixUsage));

			// bytesToSkip < _header.PrefixUsage

			Debug.Assert(Prefix != null);

			var prefixPart = Header.PrefixUsage - bytesToSkip;

			var sliceSize = prefixPart + Header.NonPrefixedDataSize;
			var sliceData = new byte[sliceSize];

			fixed (byte* slicePtr = sliceData)
			{
				if (Prefix.Value == null)
                    MemoryUtils.Copy(slicePtr, Prefix.ValuePtr + bytesToSkip, prefixPart);
				else
				{
					fixed (byte* prefixVal = Prefix.Value)
                        MemoryUtils.Copy(slicePtr, prefixVal + bytesToSkip, prefixPart);
				}
			}

			NonPrefixedData.CopyTo(0, sliceData, prefixPart, sliceSize - prefixPart);

			return new Slice(sliceData);
		}

		protected override int CompareData(MemorySlice other, ushort size)
		{
			var prefixedSlice = other as PrefixedSlice;

			if (prefixedSlice != null)
				return PrefixedSliceComparisonMethods.Compare(this, prefixedSlice, MemoryUtils.MemoryComparerInstance, size);

			var slice = other as Slice;

			if (slice != null)
			{
				return PrefixedSliceComparisonMethods.Compare(slice, this, MemoryUtils.MemoryComparerInstance, size) * -1;
			}

			throw new NotSupportedException("Cannot compare because of unknown slice type: " + other.GetType());
		}

		protected override int CompareData(MemorySlice other, PrefixedSliceComparer cmp, ushort size)
		{
			var prefixedSlice = other as PrefixedSlice;

			if (prefixedSlice != null)
				return PrefixedSliceComparisonMethods.Compare(this, prefixedSlice, cmp, size);

			var slice = other as Slice;

			if (slice != null)
			{
				return PrefixedSliceComparisonMethods.Compare(slice, this, cmp, size) * -1;
			}

			throw new NotSupportedException("Cannot compare because of unknown slice type: " + other.GetType());
		}

		public override string ToString()
		{
			if (Prefix != null)
			{
				if(Prefix.Value == null)
					return new Slice(Prefix.ValuePtr, Header.PrefixUsage) + NonPrefixedData.ToString();

				return new Slice(Prefix.Value, Header.PrefixUsage) + NonPrefixedData.ToString();
			}

			if (Header.PrefixId == NonPrefixedId)
				return NonPrefixedData.ToString();

			return string.Format("prefix_id: {0} [usage: {1}], non_prefixed: {2}", Header.PrefixId, Header.PrefixUsage, NonPrefixedData);
		}
	}
}