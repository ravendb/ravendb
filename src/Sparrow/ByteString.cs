using Sparrow.Binary;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Sparrow.Collections;
using Sparrow.Extensions;
using Sparrow.Global;
using Sparrow.Json;
using Sparrow.LowMemory;
using Sparrow.Threading;
using Sparrow.Utils;

namespace Sparrow
{
    [Flags]
    public enum ByteStringType : byte
    {
        Immutable = 0x00, // This is a shorthand for an internal-immutable string. 
        Mutable = 0x01,
        External = 0x02,
        Disposed = 0x04,
        Reserved2 = 0x08, // This bit is reserved for future uses.

        // These flags are unused and can be used by users to store custom information on the instance.
        UserDefined1 = 0x10,
        UserDefined2 = 0x20,
        UserDefined3 = 0x40,
        UserDefined4 = 0x80,

        /// <summary>
        /// Use this value to mask out the user defined bits using the (Bitwise AND) operator.
        /// </summary>
        ByteStringMask = 0x0F,

        /// <summary>
        /// Use this value to mask out the ByteStringType bits using the (Bitwise AND) operator.
        /// </summary>
        UserDefinedMask = 0xF0
    }

    [StructLayout(LayoutKind.Sequential)]
    unsafe struct ByteStringStorage
    {
        /// <summary>
        /// The actual type for the byte string
        /// </summary>
        public ByteStringType Flags;

        /// <summary>
        /// The actual length of the byte string
        /// </summary>
        public int Length;

        /// <summary>
        /// This is the pointer to the start of the byte stream. 
        /// </summary>
        public byte* Ptr;

        /// <summary>
        /// This is the total storage size for this byte string. Length will always be smaller than Size - 1.
        /// </summary>
        public int Size;

#if VALIDATE
        public const ulong NullKey = unchecked((ulong)-1);

        /// <summary>
        /// The validation key for the storage value.
        /// </summary>
        public ulong Key;
#endif

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong GetContentHash()
        {
            // Given how the size of slices can vary it is better to lose a bit (10%) on smaller slices 
            // (less than 20 bytes) and to win big on the bigger ones. 
            //
            // After 24 bytes the gain is 10%
            // After 64 bytes the gain is 2x
            // After 128 bytes the gain is 4x.
            //
            // We should control the distribution of this over time.

            // JIT will remove the corresponding line based on the target architecture using dead code removal.
            if (IntPtr.Size == 4)
                return Hashing.XXHash32.CalculateInline(Ptr, Length);
            else
                return Hashing.XXHash64.CalculateInline(Ptr, (ulong)Length);
        }
    }

    public unsafe struct ByteString : IEquatable<ByteString>, IPointerType
    {
        internal ByteStringStorage* _pointer;

#if VALIDATE
        internal ByteString(ByteStringStorage* ptr)
        {
            this._pointer = ptr;
            this.Key = ptr->Key; // We store the storage key
        }

        internal readonly ulong Key;
#else
        internal ByteString(ByteStringStorage* ptr)
        {
            _pointer = ptr;
        }
#endif
        public ByteStringType Flags
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                Debug.Assert(HasValue);
                EnsureIsNotBadPointer();

                return _pointer->Flags;
            }
        }

        public byte* Ptr
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                Debug.Assert(HasValue);
                EnsureIsNotBadPointer();

                return _pointer->Ptr;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetUserDefinedFlags(ByteStringType flags)
        {
            if ((flags & ByteStringType.ByteStringMask) == 0)
            {
                _pointer->Flags |= flags;
                return;
            }

            ThrowFlagsWithReservedBits();
        }

        private void ThrowFlagsWithReservedBits()
        {
            throw new ArgumentException("The flags passed contains reserved bits.");
        }

        public bool IsMutable
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                Debug.Assert(HasValue);
                EnsureIsNotBadPointer();

                return (_pointer->Flags & ByteStringType.Mutable) != 0;
            }
        }

        public bool IsExternal
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                Debug.Assert(HasValue);
                EnsureIsNotBadPointer();

                return (_pointer->Flags & ByteStringType.External) != 0;
            }
        }

        public int Length
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (_pointer == null)
                    return 0;

                EnsureIsNotBadPointer();

                return _pointer->Length;
            }
        }

        public bool HasValue
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return _pointer != null && _pointer->Flags != ByteStringType.Disposed;
            }
        }

        public byte this[int index]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                Debug.Assert(HasValue);
                EnsureIsNotBadPointer();

                return *(_pointer->Ptr + (sizeof(byte) * index));
            }
        }

        public void CopyTo(int from, byte* dest, int offset, int count)
        {
            Debug.Assert(HasValue);

            if (from + count > _pointer->Length)
                throw new ArgumentOutOfRangeException(nameof(from), "Cannot copy data after the end of the slice");

            EnsureIsNotBadPointer();
            Memory.Copy(dest + offset, _pointer->Ptr + from, count);
        }

        public void CopyTo(byte* dest)
        {
            Debug.Assert(HasValue);

            EnsureIsNotBadPointer();
            Memory.Copy(dest, _pointer->Ptr, _pointer->Length);
        }

        public void CopyTo(byte[] dest)
        {
            Debug.Assert(HasValue);

            EnsureIsNotBadPointer();
            fixed (byte* p = dest)
            {
                Memory.Copy(p, _pointer->Ptr, _pointer->Length);
            }
        }

#if VALIDATE

        [Conditional("VALIDATE")]
        internal void EnsureIsNotBadPointer()
        {
            if (_pointer->Address == null)
                throw new InvalidOperationException("The inner storage pointer is not initialized. This is a defect on the implementation of the ByteStringContext class");

            if (_pointer->Key == ByteStringStorage.NullKey)
                throw new InvalidOperationException("The memory referenced has already being released. This is a dangling pointer. Check your .Release() statements and aliases in the calling code.");

            if ( this.Key != _pointer->Key)
            {
                if (this.Key >> 16 != _pointer->Key >> 16)
                    throw new InvalidOperationException("The owner context for the ByteString and the unmanaged storage are different. Make sure you havent killed the allocator and kept a reference to the ByteString outside of its scope.");

                Debug.Assert((this.Key & 0x0000000FFFFFFFF) != (_pointer->Key & 0x0000000FFFFFFFF));
                throw new InvalidOperationException("The key for the ByteString and the unmanaged storage are different. This is a dangling pointer. Check your .Release() statements and aliases in the calling code.");                                    
            }
        }

#else
        [Conditional("VALIDATE")]
        internal void EnsureIsNotBadPointer() { }
#endif

        public void CopyTo(int from, byte[] dest, int offset, int count)
        {
            Debug.Assert(HasValue);

            if (from + count > _pointer->Length)
                throw new ArgumentOutOfRangeException(nameof(from), "Cannot copy data after the end of the slice");
            if (offset + count > dest.Length)
                throw new ArgumentOutOfRangeException(nameof(from), "Cannot copy data after the end of the buffer");

            EnsureIsNotBadPointer();
            fixed (byte* p = dest)
            {
                Memory.Copy(p + offset, _pointer->Ptr + from, count);
            }
        }

        public override string ToString()
        {
            if (!HasValue)
                return string.Empty;

            EnsureIsNotBadPointer();

            return UTF8Encoding.UTF8.GetString(_pointer->Ptr, _pointer->Length);
        }

        public string ToString(UTF8Encoding encoding)
        {
            if (!HasValue)
                return string.Empty;

            EnsureIsNotBadPointer();

            return encoding.GetString(_pointer->Ptr, _pointer->Length);
        }

        public int IndexOf(byte c)
        {
            for (int i = 0; i < Length; i++)
            {
                if (this[i] == c)
                    return i;
            }
            return -1;
        }

        public string Substring(int length)
        {
            if (!HasValue)
                return string.Empty;

            EnsureIsNotBadPointer();

            var encoding = Encodings.Utf8;
            return encoding.GetString(_pointer->Ptr, length);
        }

        public void Truncate(int newSize)
        {
            EnsureIsNotBadPointer();

            if(_pointer->Size < newSize || newSize < 0)
                ThrowInvalidSize();

            _pointer->Length = newSize;
        }

        private static void ThrowInvalidSize()
        {
            throw new ArgumentOutOfRangeException("newSize", "must be within the existing string limits");
        }


        public string ToString(Encoding encoding)
        {
            if (!HasValue)
                return string.Empty;

            EnsureIsNotBadPointer();

            return encoding.GetString(_pointer->Ptr, _pointer->Length);
        }

        [Obsolete("This is a reference comparison. Use SliceComparer or ByteString.Match instead.", error: true)]
#pragma warning disable CS0809
        public override bool Equals(object obj)
#pragma warning restore CS0809
        {
            return obj is ByteString && this == (ByteString)obj;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong GetContentHash()
        {
            // Given how the size of slices can vary it is better to lose a bit (10%) on smaller slices 
            // (less than 20 bytes) and to win big on the bigger ones. 
            //
            // After 24 bytes the gain is 10%
            // After 64 bytes the gain is 2x
            // After 128 bytes the gain is 4x.
            //
            // We should control the distribution of this over time.

            if (_pointer == null)
                return 0;

            return _pointer->GetContentHash();
        }

        public override int GetHashCode()
        {
            return (int)GetContentHash();
        }

        [Obsolete("This is a reference comparison. Use SliceComparer or ByteString.Match instead.", error: true)]
        public static bool operator ==(ByteString x, ByteString y)
        {
            return x._pointer == y._pointer;
        }

        [Obsolete("This is a reference comparison. Use SliceComparer or ByteString.Match instead.", error: true)]
        public static bool operator !=(ByteString x, ByteString y)
        {
            return !(x == y);
        }

        [Obsolete("This is a reference comparison. Use SliceComparer or ByteString.Match instead.", error: true)]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(ByteString other)
        {
            return this == other;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Match(ByteString other)
        {
            return Length == other.Length &&
                   Memory.Compare(Ptr, other.Ptr, Length) == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Match(LazyStringValue other)
        {
            return Length == other.Length &&
                   Memory.Compare(Ptr, other.Buffer, Length) == 0;
        }
    }

    public sealed class ByteStringContext : ByteStringContext<ByteStringContext.WithPooling>
    {
        private struct FragmentPool : IFragmentAllocatorOptions
        {
            public int ReuseBlocksBiggerThan => 64 * Constants.Size.Kilobyte;
            public int BlockSize => default(FixedSizeThreadAffinePoolAllocator.Default).BlockSize;

            public bool HasOwnership => true;
            public IAllocatorComposer<Pointer> CreateAllocator()
            {
                var allocator = new Allocator<NativeAllocator<Direct>>();
                allocator.Initialize(default(Direct));
                return allocator;
            }

            public void ReleaseAllocator(IAllocatorComposer<Pointer> allocator, bool disposing)
            {
                allocator.Dispose(disposing);
            }
        }

        public struct WithoutPooling : IPoolAllocatorOptions
        {
            public bool HasOwnership => false;
            public IAllocatorComposer<Pointer> CreateAllocator()
            {
                var allocator = new Allocator<FragmentAllocator<FragmentPool>>();
                allocator.Initialize(default(FragmentPool));
                return allocator;
            }

            /// <summary>
            /// By default whenever we create an allocator we are going to dispose it too when the time comes.
            /// </summary>
            /// <param name="allocator">the allocator to dispose.</param>
            public void ReleaseAllocator(IAllocatorComposer<Pointer> allocator, bool disposing)
            {
                allocator.Dispose(disposing);
            }

            public int BlockSize => default(FragmentPool).BlockSize;
            public int MaxBlockSize => 0;
            public int MaxPoolSizeInBytes => 0; // We are effectively disabling the pooling. 
        }

        public struct ElectricFence : IPoolAllocatorOptions
        {
            public bool HasOwnership => false;
            public IAllocatorComposer<Pointer> CreateAllocator()
            {
                var allocator = new Allocator<NativeAllocator<NativeAllocator.ElectricFence>>();
                allocator.Initialize(default(NativeAllocator.ElectricFence));
                return allocator;
            }

            /// <summary>
            /// By default whenever we create an allocator we are going to dispose it too when the time comes.
            /// </summary>
            /// <param name="allocator">the allocator to dispose.</param>
            public void ReleaseAllocator(IAllocatorComposer<Pointer> allocator, bool disposing)
            {
                allocator.Dispose(disposing);
            }

            public int BlockSize => default(FragmentPool).BlockSize;
            public int MaxBlockSize => 0;
            public int MaxPoolSizeInBytes => 0; // We are effectively disabling the pooling. 
        }

        public struct WithPooling : IPoolAllocatorOptions
        {
            public bool HasOwnership => false;
            public IAllocatorComposer<Pointer> CreateAllocator()
            {
                var allocator = new Allocator<FragmentAllocator<FragmentPool>>();
                allocator.Initialize(default(FragmentPool));
                return allocator;
            }

            /// <summary>
            /// By default whenever we create an allocator we are going to dispose it too when the time comes.
            /// </summary>
            /// <param name="allocator">the allocator to dispose.</param>
            public void ReleaseAllocator(IAllocatorComposer<Pointer> allocator, bool disposing)
            {
                allocator.Dispose(disposing);
            }

            public int BlockSize => default(FragmentPool).BlockSize;
            public int MaxBlockSize => 1 * Constants.Size.Megabyte;
            public int MaxPoolSizeInBytes => 64 * Constants.Size.Megabyte;
        }

        public struct Direct : IPoolAllocatorOptions, INativeOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => false;

            public bool HasOwnership => false;
            public IAllocatorComposer<Pointer> CreateAllocator()
            {
                var allocator = new Allocator<NativeAllocator<Direct>>();
                allocator.Initialize(default(Direct));
                return allocator;
            }

            /// <summary>
            /// By default whenever we create an allocator we are going to dispose it too when the time comes.
            /// </summary>
            /// <param name="allocator">the allocator to dispose.</param>
            public void ReleaseAllocator(IAllocatorComposer<Pointer> allocator, bool disposing)
            {
                allocator.Dispose(disposing);
            }

            public int BlockSize => default(FragmentPool).BlockSize;
            public int MaxBlockSize => 1 * Constants.Size.Megabyte;
            public int MaxPoolSizeInBytes => 64 * Constants.Size.Megabyte;
        }

        public struct Static : IPoolAllocatorOptions, INativeOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => false;

            public bool HasOwnership => false;
            public IAllocatorComposer<Pointer> CreateAllocator()
            {
                var allocator = new Allocator<NativeAllocator<Direct>>();
                allocator.Initialize(default(Direct));
                return allocator;
            }

            public void ReleaseAllocator(IAllocatorComposer<Pointer> allocator, bool disposing)
            {
                // For all uses and purposes the underlying Native Allocator will be finalized as Statics should
                // never deallocate until the process dies. This way we also skip the leak checks. 
                allocator.Dispose(false);
            }

            public int BlockSize => default(FragmentPool).BlockSize;
            public int MaxBlockSize => 1 * Constants.Size.Megabyte;
            public int MaxPoolSizeInBytes => 64 * Constants.Size.Megabyte;
        }
    }

    public unsafe class ByteStringContext<TOptions> : IDisposable where TOptions : struct, IPoolAllocatorOptions
    {
        private readonly TOptions _options;
        private PoolAllocator<TOptions> _allocator;       

        private long _totalAllocated, _currentlyAllocated;
        private readonly SingleUseFlag _disposeFlag = new SingleUseFlag();

        public ByteStringContext()
        {
            _allocator = new PoolAllocator<TOptions>();
            _allocator.Initialize(ref _allocator);
            _allocator.Configure(ref _allocator, ref _options);

            PrepareForValidation();
        }

        public void Reset()
        {
            if (_disposeFlag.IsRaised())
                ThrowObjectDisposed();
            
            _currentlyAllocated = 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public InternalScope Allocate(int length, out ByteString output)
        {
            output = AllocateInternal(length, ByteStringType.Mutable);
            return new InternalScope(this, output);
        }

        public override string ToString()
        {
            return $"Allocated {Sizes.Humane(_currentlyAllocated)} / {Sizes.Humane(_totalAllocated)}";
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ByteString AllocateExternal(byte* valuePtr, int size, ByteStringType type)
        {
            Debug.Assert((type & ByteStringType.External) != 0, "This allocation routine is only for use with external storage byte strings.");
            Debug.Assert(size >= 0);

            _totalAllocated += sizeof(ByteStringStorage);
            BlockPointer ptr = _allocator.Allocate(ref _allocator, sizeof(ByteStringStorage));
            ByteStringStorage* storagePtr = (ByteStringStorage*)ptr.Address;   

            storagePtr->Flags = type;
            storagePtr->Length = size;
            storagePtr->Ptr = valuePtr;
            storagePtr->Size = ptr.BlockSize;

            // We are registering the storage for validation here. Not the ByteString itself
            RegisterForValidation(storagePtr);

            return new ByteString(storagePtr);
        }

        private ByteString AllocateInternal(int length, ByteStringType type)
        {
            if (_disposeFlag.IsRaised())
                ThrowObjectDisposed();

            Debug.Assert(length >= 0);
            Debug.Assert((type & ByteStringType.External) == 0, "This allocation routine is only for use with internal storage byte strings.");
            type &= ~ByteStringType.External; // We are allocating internal, so we will force it (even if we are checking for it in debug).

            int size = length + sizeof(ByteStringStorage);

            BlockPointer ptr = _allocator.Allocate(ref _allocator, size);

            Debug.Assert(length <= ptr.BlockSize - sizeof(ByteStringStorage));

            var basePtr = (ByteStringStorage*)ptr.Address;
            basePtr->Flags = type;
            basePtr->Length = length;
            basePtr->Ptr = (byte*)ptr.Address + sizeof(ByteStringStorage);            
            basePtr->Size = ptr.BlockSize;            

            // We are registering the storage for validation here. Not the ByteString itself
            RegisterForValidation(basePtr);

            return new ByteString(basePtr);
        }

        [ThreadStatic]
        public static char[] ToLowerTempBuffer;

        /// <summary>
        /// Mutate the string to lower case
        /// </summary>
        public void ToLowerCase(ref ByteString str)
        {
            if (str.Length == 0)
                return;

            if (str.IsMutable == false)
                throw new InvalidOperationException("Cannot mutate an immutable ByteString");

            var charCount = Encodings.Utf8.GetCharCount(str._pointer->Ptr, str.Length);
            if (ToLowerTempBuffer == null || ToLowerTempBuffer.Length < charCount)
            {
                ToLowerTempBuffer = new char[Bits.PowerOf2(charCount)];
            }

            fixed (char* pChars = ToLowerTempBuffer)
            {
                charCount = Encodings.Utf8.GetChars(str._pointer->Ptr, str.Length, pChars, ToLowerTempBuffer.Length);
                for (int i = 0; i < charCount; i++)
                {
                    ToLowerTempBuffer[i] = char.ToLowerInvariant(ToLowerTempBuffer[i]);
                }
                var byteCount = Encodings.Utf8.GetByteCount(pChars, charCount);
                if (// we can't mutate external memory!
                    str.IsExternal ||
                    // calling to lower has increased the size, and we can't fit in the space
                    // provided, so we must allocate a new string here
                    byteCount > str._pointer->Size)
                {
                    Allocate(byteCount, out str);
                }
                str._pointer->Length = Encodings.Utf8.GetBytes(pChars, charCount, str._pointer->Ptr, str._pointer->Size);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReleaseExternal(ref ByteString value)
        {
            if (_disposeFlag.IsRaised())
                ThrowObjectDisposed();
            
            Debug.Assert(value._pointer != null, "Pointer cannot be null. You have a defect in your code.");

            if (value._pointer == null) // this is a safe-guard on Release, it is better to not release the memory than fail
                return;

            Debug.Assert(value.Length >= 0);
            Debug.Assert(value.IsExternal, "Cannot release as external an internal pointer.");

            // We are releasing, therefore we should validate among other things if an immutable string changed and if we are the owners.
            ValidateAndUnregister(value);

            value._pointer->Flags = ByteStringType.Disposed;

            BlockPointer ptr = new BlockPointer(value._pointer, value._pointer->Size, value._pointer->Length);

#if VALIDATE
            // This must happen before the actual release, in case that the fragment allocator would decide to actually kill the memory instead.        

            // Setting the null key ensures that in between we can validate that no further deallocation
            // happens on this memory segment.
            value._pointer->Key = ByteStringStorage.NullKey;

            // Setting the length to zero ensures that the hash returns 0 and do not 
            // fail with an AccessViolationException because there is garbage stored here.
            value._pointer->Length = 0;
#endif

            _allocator.Release(ref _allocator, ref ptr);

            // WE WANT it to happen, no matter what. 
            value._pointer = null;
        }

        public void Release(ref ByteString value)
        {
            if (_disposeFlag.IsRaised())
                ThrowObjectDisposed();
            
            Debug.Assert(value._pointer != null, "Pointer cannot be null. You have a defect in your code.");
            if (value._pointer == null) // this is a safe-guard on Release, it is better to not release the memory than fail
                return;

            Debug.Assert(value.Length >= 0);
            Debug.Assert(value._pointer->Flags != ByteStringType.Disposed, "Double free");
            Debug.Assert(!value.IsExternal, "Cannot release as internal an external pointer.");

            _currentlyAllocated -= value._pointer->Size;

            // We are releasing, therefore we should validate among other things if an immutable string changed and if we are the owners.
            ValidateAndUnregister(value);

            BlockPointer ptr = new BlockPointer(value._pointer, value._pointer->Size, value._pointer->Length);

#if VALIDATE
            // This must happen before the actual release, in case that the fragment allocator would decide to actually kill the memory instead.        

            // Setting the null key ensures that in between we can validate that no further deallocation
            // happens on this memory segment.
            value._pointer->Key = ByteStringStorage.NullKey;

            // Setting the length to zero ensures that the hash returns 0 and do not 
            // fail with an AccessViolationException because there is garbage stored here.
            value._pointer->Length = 0;
#endif

            _allocator.Release(ref _allocator, ref ptr);

            // WE WANT it to happen, no matter what. 
            value._pointer = null;
        }        

        private static void ThrowObjectDisposed()
        {
            throw new ObjectDisposedException("ByteStringContext");
        }

        public ByteString Skip(ByteString value, int bytesToSkip, ByteStringType type = ByteStringType.Mutable)
        {
            Debug.Assert(value._pointer != null, "ByteString cant be null.");

            if (_disposeFlag.IsRaised())
                ThrowObjectDisposed();

            if (bytesToSkip < 0)
                throw new ArgumentException($"'{nameof(bytesToSkip)}' cannot be smaller than 0.");

            if (bytesToSkip > value.Length)
                throw new ArgumentException($"'{nameof(bytesToSkip)}' cannot be bigger than '{nameof(value)}.Length' 0.");

            int size = value.Length - bytesToSkip;
            var result = AllocateInternal(size, type);
            Memory.Copy(result._pointer->Ptr, value._pointer->Ptr + bytesToSkip, size);

            RegisterForValidation(result);
            return result;
        }

        public ByteString Clone(ByteString value, ByteStringType type = ByteStringType.Mutable)
        {
            Debug.Assert(value._pointer != null, $"{nameof(value)} cant be null.");

            var result = AllocateInternal(value.Length, type);
            Memory.Copy(result._pointer->Ptr, value._pointer->Ptr, value._pointer->Length);

            RegisterForValidation(result);
            return result;
        }

        public InternalScope From(string value, out ByteString str)
        {
            return From(value, ByteStringType.Mutable, out str);
        }
        
        public InternalScope From(char* value, int charCount, ByteStringType type, out ByteString str)
        {
            Debug.Assert(value != null, $"{nameof(value)} cant be null.");

            var byteCount = Encodings.Utf8.GetByteCount(value, charCount);
            str = AllocateInternal(byteCount, type);
            int length = Encodings.Utf8.GetBytes(value, charCount, str.Ptr, byteCount);

            // We can do this because it is internal. See if it makes sense to actually give this ability. 
            str._pointer->Length = length;

            RegisterForValidation(str);
            return new InternalScope(this, str);
        }

        public InternalScope From(string value, ByteStringType type, out ByteString str)
        {
            Debug.Assert(value != null, $"{nameof(value)} cant be null.");

            var byteCount = value.GetUtf8MaxSize();
            str = AllocateInternal(byteCount, type);
            fixed (char* ptr = value)
            {
                int length = Encodings.Utf8.GetBytes(ptr, value.Length, str.Ptr, byteCount);

                // We can do this because it is internal. See if it makes sense to actually give this ability. 
                str._pointer->Length = length;
            }

            RegisterForValidation(str);
            return new InternalScope(this, str);
        }

        public InternalScope From(string value, Encoding encoding, out ByteString str)
        {
            return From(value, encoding, ByteStringType.Immutable, out str);
        }

        public InternalScope From(string value, Encoding encoding, ByteStringType type, out ByteString str)
        {
            Debug.Assert(value != null, $"{nameof(value)} cant be null.");

            var byteCount = value.GetUtf8MaxSize();

            str = AllocateInternal(byteCount, type);
            fixed (char* ptr = value)
            {
                int length = encoding.GetBytes(ptr, value.Length, str.Ptr, byteCount);

                // We can do this because it is internal. See if it makes sense to actually give this ability. 
                str._pointer->Length = length;
            }

            RegisterForValidation(str);
            return new InternalScope(this, str);
        }

        public InternalScope From(byte[] value, int offset, int count, ByteStringType type, out ByteString str)
        {
            Debug.Assert(value != null, $"{nameof(value)} cant be null.");

            str = AllocateInternal(count, type);
            fixed (byte* ptr = value)
            {
                Memory.Copy(str._pointer->Ptr, ptr + offset, count);
            }

            RegisterForValidation(str);
            return new InternalScope(this, str);
        }

        public InternalScope From(byte[] value, int offset, int count, out ByteString str)
        {
            return From(value, offset, count, ByteStringType.Immutable, out str);
        }

        public InternalScope From(byte[] value, int size, ByteStringType type, out ByteString str)
        {
            Debug.Assert(value != null, $"{nameof(value)} cant be null.");

            str = AllocateInternal(size, type);
            fixed (byte* ptr = value)
            {
                Memory.Copy(str._pointer->Ptr, ptr, size);
            }

            RegisterForValidation(str);
            return new InternalScope(this, str);
        }

        public InternalScope From(byte[] value, int size, out ByteString str)
        {
            return From(value, size, ByteStringType.Immutable, out str);
        }

        public InternalScope From(int value, out ByteString str)
        {
            return From(value, ByteStringType.Immutable, out str);
        }

        public InternalScope From(int value, ByteStringType type, out ByteString str)
        {
            str = AllocateInternal(sizeof(int), type);
            ((int*)str._pointer->Ptr)[0] = value;

            RegisterForValidation(str);
            return new InternalScope(this, str);
        }

        public InternalScope From(long value, out ByteString str)
        {
            return From(value, ByteStringType.Immutable, out str);
        }

        public InternalScope From(long value, ByteStringType type, out ByteString str)
        {
            str = AllocateInternal(sizeof(long), type);
            ((long*)str._pointer->Ptr)[0] = value;

            RegisterForValidation(str);
            return new InternalScope(this, str);
        }

        public InternalScope From(short value, out ByteString str)
        {
            return From(value, ByteStringType.Immutable, out str);
        }

        public InternalScope From(short value, ByteStringType type, out ByteString str)
        {
            str = AllocateInternal(sizeof(short), type);
            ((short*)str._pointer->Ptr)[0] = value;

            RegisterForValidation(str);
            return new InternalScope(this, str);
        }

        public InternalScope From(byte value, ByteStringType type, out ByteString str)
        {
            str = AllocateInternal(1, type);
            str._pointer->Ptr[0] = value;

            RegisterForValidation(str);
            return new InternalScope(this, str);
        }

        public InternalScope From(byte value, out ByteString str)
        {
            return From(value, ByteStringType.Immutable, out str);
        }

        public InternalScope From(byte* valuePtr, int size, out ByteString str)
        {
            return From(valuePtr, size, ByteStringType.Immutable, out str);
        }

        public InternalScope From(byte* valuePtr, int size, ByteStringType type, out ByteString str)
        {
            Debug.Assert(valuePtr != null, $"{nameof(valuePtr)} cant be null.");
            Debug.Assert((type & ByteStringType.External) == 0, $"{nameof(From)} is not expected to be called with the '{nameof(ByteStringType.External)}' requested type, use {nameof(FromPtr)} instead.");

            str = AllocateInternal(size, type);
            Memory.Copy(str._pointer->Ptr, valuePtr, size);

            RegisterForValidation(str);
            return new InternalScope(this, str);
        }

        /// <summary>
        /// This scope should only be used whenever you can not determine
        /// whether the scope is internal or external, as the dispose cost
        /// is higher than either of the options.
        /// </summary>
        public struct Scope : IDisposable
        {
            private ByteStringContext<TOptions> _parent;
            private ByteString _str;

            public Scope(ByteStringContext<TOptions> parent, ByteString str)
            {
                _parent = parent;
                _str = str;
            }

            public void Dispose()
            {
                if (_parent != null)
                {
                    if (_str.IsExternal)
                        _parent.ReleaseExternal(ref _str);
                    else
                        _parent.Release(ref _str);
                }
                _parent = null;
            }
        }

        public struct InternalScope : IDisposable
        {
            private ByteStringContext<TOptions> _parent;
            private ByteString _str;

            public InternalScope(ByteStringContext<TOptions> parent, ByteString str)
            {
                _parent = parent;
                _str = str;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Dispose()
            {
                _parent?.Release(ref _str);
                _parent = null;
            }

            public static implicit operator Scope(InternalScope scope)
            {
                return new Scope(scope._parent, scope._str);
            }
        }

        public struct ExternalScope : IDisposable
        {
            private ByteStringContext<TOptions> _parent;
            private ByteString _str;

            public ExternalScope(ByteStringContext<TOptions> parent, ByteString str)
            {
                _parent = parent;
                _str = str;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Dispose()
            {
                _parent?.ReleaseExternal(ref _str);
                _parent = null;
            }

            public static implicit operator Scope(ExternalScope scope)
            {
                return new Scope(scope._parent, scope._str);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ExternalScope FromPtr(byte* valuePtr, int size,
            ByteStringType type,
            out ByteString str)
        {
            Debug.Assert(valuePtr != null || size == 0, $"{nameof(valuePtr)} cant be null if the size is not zero");
            Debug.Assert(size >= 0, $"{nameof(size)} cannot be negative.");

            str = AllocateExternal(valuePtr, size, type | ByteStringType.External); // We are allocating external, so we will force it (even if we are checking for it in debug).

            RegisterForValidation(str);

            return new ExternalScope(this, str);
        }

#if VALIDATE

        private static int globalContextId;
        private int _allocationCount;

        protected int ContextId;

        private void PrepareForValidation()
        {
            this.ContextId = Interlocked.Increment(ref globalContextId);
        }

        private void RegisterForValidation(void* storage)
        {
            // There shouldn't be reuse for the storage, unless we have a different allocation on reused memory.
            // Therefore, monotonically increasing the key we ensure that we can check when we have dangling pointers in our code.
            // We use interlocked in order to avoid validation bugs when validating (we are playing it safe).
            ((ByteStringStorage*)storage)->Key = (ulong)(((long)this.ContextId << 32) + Interlocked.Increment(ref _allocationCount));
        }

        private void RegisterForValidation(ByteString value)
        {
            value.EnsureIsNotBadPointer();

            if (!value.IsMutable)
            {
                ulong index = (ulong)value._pointer;
                ulong hash = value.GetContentHash();

                _immutableTracker[index] = new Tuple<IntPtr, ulong, string>(new IntPtr(value._pointer), hash, Environment.StackTrace);
            }                
        }        

        private void ValidateAndUnregister(ByteString value)
        {
            value.EnsureIsNotBadPointer();

            if (value._pointer->Key == ByteStringStorage.NullKey)
                throw new ByteStringValidationException("Trying to release an alias of an already removed object. You have a dangling pointer in hand.");

            if (value._pointer->Key >> 32 != (ulong)this.ContextId)
                throw new ByteStringValidationException("The owner of the ByteString is a different context. You are mixing contexts, which has undefined behavior.");

            if (!value.IsMutable)
            {                
                ValidateAndUnregister(value._pointer);
            }
        }

        private void ValidateAndUnregister(ByteStringStorage* value)
        {
            ulong index = (ulong)value;
            ulong hash = value->GetContentHash();

            try
            {                
                Tuple<IntPtr, ulong, string> item;
                if (!_immutableTracker.TryGetValue(index, out item))
                    throw new ByteStringValidationException($"The ByteStream is being released as Immutable, but it was not registered. Potential buffer overflow detected.");

                if (hash != item.Item2)
                    throw new ByteStringValidationException($"The ByteString in location {(ulong)value} and size {value->Length} was modified but it was created as immutable. {Environment.NewLine} {item.Item3}" );
            }
            finally
            {
                _immutableTracker.Remove(index);
            }
        }

        private readonly Dictionary<ulong, Tuple<IntPtr, ulong, string>> _immutableTracker = new Dictionary<ulong, Tuple<IntPtr, ulong, string>>();

#else
        [Conditional("VALIDATE")]
        private void PrepareForValidation() { }

        [Conditional("VALIDATE")]
        private void RegisterForValidation(void* _) { }

        [Conditional("VALIDATE")]
        private void RegisterForValidation(ByteString _) { }

        [Conditional("VALIDATE")]
        private void ValidateAndUnregister(ByteString _) { }

#endif

        ~ByteStringContext()
        {
            try
            {                
                Dispose(false);
            }
            catch (ObjectDisposedException)
            {
                // This is expected, we might be calling the finalizer on an object that
                // was already disposed, we don't want to error here because of this
            }
        }

        protected void Dispose(bool disposing)
        {
            if (!_disposeFlag.Raise())
                return;

            _allocator.Dispose(ref _allocator, disposing);
            GC.SuppressFinalize(this);
        }

        public void Dispose()
        {
            Dispose(true);            
        }
    }

    public class ByteStringValidationException : Exception
    {
        public ByteStringValidationException()
        {
        }

        public ByteStringValidationException(string message)
            : base(message)
        {
        }

        public ByteStringValidationException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
