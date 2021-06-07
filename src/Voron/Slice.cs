using Sparrow;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Threading;

namespace Voron
{
    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct Slice
    {
        public ByteString Content;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Slice(SliceOptions options, ByteString content)
        {
            Content = content;
            Content.SetUserDefinedFlags((ByteStringType)options);               
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Slice(ByteString content)
        {
            Content = content;
        }
        
        public ReadOnlySpan<byte> AsReadOnlySpan() => new ReadOnlySpan<byte>(Content.Ptr, Content.Length);

        public bool HasValue => Content.HasValue;

        public int Size
        {
            get
            {
                Debug.Assert(Content.Length >= 0);
                return Content.Length;
            }
        }

        public SliceOptions Options => (SliceOptions) (Content.Flags & ByteStringType.UserDefinedMask);

        public byte this[int index]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                Debug.Assert(Content.Ptr != null, "Uninitialized slice!");

                if (!Content.HasValue)
                    throw new InvalidOperationException("Uninitialized slice!");

                return *(Content.Ptr + (sizeof(byte) * index));
            }
        }

        public AllocatedMemoryData CloneToJsonContext(JsonOperationContext context, out Slice slice)
        {
            var mem = context.GetMemory(Size);
            Memory.Copy(mem.Address, Content._pointer, Size);
            slice = new Slice(new ByteString((ByteStringStorage*)mem.Address));
            return mem;
        }

        public Slice Clone(ByteStringContext context, ByteStringType type = ByteStringType.Mutable)
        {
            return new Slice(context.Clone(this.Content, type));
        }

        public Slice Skip(ByteStringContext context, int bytesToSkip, ByteStringType type = ByteStringType.Mutable)
        {
            return new Slice(context.Skip(this.Content, bytesToSkip, type));       
        }

        public void CopyTo(int from, byte* dest, int offset, int count)
        {
            this.Content.CopyTo(from, dest, offset, count);
        }

        public void CopyTo(byte* dest)
        {
            this.Content.CopyTo(dest);
        }   
         
        public void CopyTo(byte[] dest)
        {
            this.Content.CopyTo(dest);
        }

        public void CopyTo(int from, byte[] dest, int offset, int count)
        {
            this.Content.CopyTo(from, dest, offset, count);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.InternalScope From(ByteStringContext context, string value, out Slice str)
        {
            return From(context, value, ByteStringType.Mutable, out str);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.InternalScope From(ByteStringContext context, string value, ByteStringType type, out Slice str)
        {
            var scope = context.From(value, type, out ByteString s);
            str = new Slice(s);
            return scope;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.InternalScope From(ByteStringContext context, string value, byte endSeparator, ByteStringType type, out Slice str)
        {
            var scope = context.From(value, endSeparator, type, out ByteString s);
            str = new Slice(s);
            return scope;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.InternalScope From(ByteStringContext context, byte[] value, out Slice str)
        {
            return From(context, value, ByteStringType.Immutable, out str);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.InternalScope From(ByteStringContext context, byte[] value, ByteStringType type, out Slice str)
        {
            var scope = context.From(value, 0, value.Length, type, out ByteString byteString);
            str = new Slice(byteString);
            return scope;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.InternalScope From(ByteStringContext context, byte[] value, int offset, int count, ByteStringType type, out Slice str)
        {
            var scope = context.From(value, offset, count, type, out ByteString byteString);
            str = new Slice(byteString);
            return scope;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.InternalScope From(ByteStringContext context, byte* value, int size, out Slice str)
        {
            return From(context, value, size, ByteStringType.Immutable, out str);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.InternalScope From(ByteStringContext context, Span<byte> value, out Slice str)
        {
            fixed (byte* k = value)
            {
                return From(context, k, value.Length, ByteStringType.Immutable, out str);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.InternalScope From(ByteStringContext context, LazyStringValue value, out Slice str)
        {
            return From(context, value.Buffer, value.Size, ByteStringType.Immutable, out str);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.InternalScope From(ByteStringContext context, byte* value, int size, ByteStringType type, out Slice str)
        {
            var scope = context.From(value, size, type, out ByteString byteString);
            str = new Slice(byteString);
            return scope;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.InternalScope From(ByteStringContext context, byte* value, int size, byte endSeparator, out Slice str)
        {
            var scope = context.From(value, size, endSeparator, ByteStringType.Immutable, out ByteString byteString);
            str = new Slice(byteString);
            return scope;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.ExternalScope External(ByteStringContext context, ByteString value, int offset, int size, out Slice slice)
        {
            Debug.Assert(offset + size <= value.Length);
            return External(context, value.Ptr + offset, size, ByteStringType.Mutable | ByteStringType.External, out slice);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.ExternalScope External(ByteStringContext context, ByteString value, int size, out Slice slice)
        {
            Debug.Assert(size <= value.Length);
            return External(context, value.Ptr, size, ByteStringType.Mutable | ByteStringType.External, out slice);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.ExternalScope External(ByteStringContext context, byte* value, int size, out Slice slice)
        {
            return External(context, value, size, ByteStringType.Mutable | ByteStringType.External, out slice);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.ExternalScope External(ByteStringContext context, Slice value, int size, out Slice slice)
        {
            return External(context, value.Content.Ptr, size, ByteStringType.Mutable | ByteStringType.External, out slice);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.ExternalScope External(ByteStringContext context, LazyStringValue value, out Slice slice)
        {
            return External(context, value.Buffer, value.Size,  ByteStringType.Mutable | ByteStringType.External, out slice);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.ExternalScope External(ByteStringContext context, byte* value, int size, ByteStringType type, out Slice slice)
        {
            var scope = context.FromPtr(value, size, type | ByteStringType.External, out ByteString str);
            slice = new Slice(str);
            return scope;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Release(ByteStringContext context)
        {
            if (Content.IsExternal)
                context.ReleaseExternal(ref Content);
            else
                context.Release(ref Content);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueReader CreateReader()
        {
            return new ValueReader(Content.Ptr, Size);
        }

        public override int GetHashCode()
        {
            return this.Content.GetHashCode();
        }

        public override string ToString()
        {
            return this.Content.ToString(Encodings.Utf8);
        }

        public Span<byte> AsSpan()
        {
            return new Span<byte>(Content.Ptr, Size);
        }
    }

    public static class Slices
    {
        private static readonly ByteStringContext SharedSliceContent = new ByteStringContext(SharedMultipleUseFlag.None);

        public static readonly Slice AfterAllKeys;
        public static readonly Slice BeforeAllKeys;
        public static readonly Slice Empty;

        static Slices()
        {
            SharedSliceContent.From(string.Empty, out ByteString empty);
            Empty = new Slice(SliceOptions.Key, empty);
            SharedSliceContent.From(string.Empty, out ByteString before);
            BeforeAllKeys = new Slice(SliceOptions.BeforeAllKeys, before);
            SharedSliceContent.From(string.Empty, out ByteString after);
            AfterAllKeys = new Slice(SliceOptions.AfterAllKeys, after);
        }
    }
}
