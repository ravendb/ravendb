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

    public unsafe struct ByteString : IEquatable<ByteString>
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
            if (_pointer->Ptr == null)
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

    public sealed unsafe class UnmanagedGlobalSegment : PooledItem
    {
        public byte* Segment;
        public readonly int Size;
        private readonly NativeMemory.ThreadStats _thread;

        public UnmanagedGlobalSegment(int size)
        {
            Size = size;
            Segment = NativeMemory.AllocateMemory(size, out _thread);
            InUse.Raise();
        }

        ~UnmanagedGlobalSegment()
        {
            try
            {
                if (Segment == null)
                    return;
                NativeMemory.Free(Segment, Size, _thread);
                Segment = null;
            }
            catch (ObjectDisposedException)
            {
                // nothing that can be done here
            }
        }

        public override void Dispose()
        {
            if (Segment == null)
                return;

            lock (this)
            {
                if (Segment == null)
                    return;
                NativeMemory.Free(Segment, Size, _thread);
                Segment = null;
                GC.SuppressFinalize(this);
            }
        }
        
    }

    public interface IByteStringAllocator
    {
        UnmanagedGlobalSegment Allocate(int size, Action allocationFailure);
        void Free(UnmanagedGlobalSegment memory);
    }

    /// <summary>
    /// This class implements a direct allocator, mostly used for testing.   
    /// </summary>
    public struct ByteStringDirectAllocator : IByteStringAllocator
    {
        public UnmanagedGlobalSegment Allocate(int size, Action allocationFailure)
        {
            try
            {
                return new UnmanagedGlobalSegment(size);
            }
            catch 
            {
                allocationFailure?.Invoke();
                throw;
            }
        }

        public void Free(UnmanagedGlobalSegment memory)
        {
            memory.Dispose();
        }
    }

    public struct ByteStringMemoryCache : IByteStringAllocator
    {
        private static readonly ThreadLocal<StackHeader<UnmanagedGlobalSegment>> SegmentsPool;
        private static readonly SharedMultipleUseFlag LowMemoryFlag;
        private static readonly LowMemoryHandler LowMemoryHandlerInstance = new LowMemoryHandler();

        public static readonly NativeMemoryCleaner<StackHeader<UnmanagedGlobalSegment>, UnmanagedGlobalSegment> Cleaner;

        private class LowMemoryHandler : ILowMemoryHandler
        {
            public void LowMemory()
            {
                if (LowMemoryFlag.Raise())
                    Cleaner.CleanNativeMemory(null);
            }

            public void LowMemoryOver()
            {
                LowMemoryFlag.Lower();
            }
        }

        static ByteStringMemoryCache()
        {
            SegmentsPool = new ThreadLocal<StackHeader<UnmanagedGlobalSegment>>(() => new StackHeader<UnmanagedGlobalSegment>(), trackAllValues: true);
            LowMemoryFlag = new SharedMultipleUseFlag();
            Cleaner = new NativeMemoryCleaner<StackHeader<UnmanagedGlobalSegment>, UnmanagedGlobalSegment>(() => SegmentsPool.Values, LowMemoryFlag, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));

            ThreadLocalCleanup.ReleaseThreadLocalState += CleanForCurrentThread;

            LowMemoryNotification.Instance.RegisterLowMemoryHandler(LowMemoryHandlerInstance);
        }

        [ThreadStatic]
        private static int _minSize;

        public UnmanagedGlobalSegment Allocate(int size, Action allocationFailure)
        {
            if (_minSize < size)
                _minSize = size;

            var stack = SegmentsPool.Value;

            while (true)
            {
                var current = stack.Head;
                if (current == null)
                    break;

                if (Interlocked.CompareExchange(ref stack.Head, current.Next, current) != current)
                    continue;

                var segment = current.Value;
                if (segment == null)
                    continue;

                if (segment.Size >= size)
                {
                    if (!segment.InUse.Raise())
                        continue;

                    return segment;
                }

                // not big enough, so we'll discard it and create a bigger instance
                // it will go into the pool afterward and be available for future use
                segment.Dispose();
            }

            // have to allocate it directly
            try
            {
                return new UnmanagedGlobalSegment(size);
            }
            catch 
            {
                allocationFailure?.Invoke();
                throw;
            }
        }

        public unsafe void Free(UnmanagedGlobalSegment memory)
        {
            if(memory.Segment == null)
                ThrowInvalidMemorySegment();

            if (_minSize > memory.Size)
            {
                memory.Dispose();
                return;
            }

            memory.InUse.Lower();
            memory.InPoolSince = DateTime.UtcNow;

            var stack = SegmentsPool.Value;

            while (true)
            {
                var current = stack.Head;
                var newHead = new StackNode<UnmanagedGlobalSegment> { Value = memory, Next = current };
                if (Interlocked.CompareExchange(ref stack.Head, newHead, current) == current)
                    return;
            }
        }

        private static void ThrowInvalidMemorySegment()
        {
            throw new InvalidOperationException("Attempt to return a memory segment that has already been disposed");
        }

        public static void CleanForCurrentThread()
        {
            if (SegmentsPool.IsValueCreated == false)
                return; // nothing to do

            var stack = SegmentsPool.Value;
            _minSize = 0;
            var current = Interlocked.Exchange(ref stack.Head, null);
            while (current != null)
            {
                current.Value?.Dispose();
                current = current.Next;
            }
        }       
    }

    public sealed class ByteStringContext : ByteStringContext<ByteStringMemoryCache>
    {
        public const int MinBlockSizeInBytes = 4 * 1024; // If this is changed, we need to change also LogMinBlockSize.
        public const int MaxAllocationBlockSizeInBytes = 256 * MinBlockSizeInBytes;
        public const int DefaultAllocationBlockSizeInBytes = 1 * MinBlockSizeInBytes;
        public const int MinReusableBlockSizeInBytes = 8;

        public ByteStringContext(SharedMultipleUseFlag lowMemoryFlag, int allocationBlockSize = DefaultAllocationBlockSizeInBytes) : base(lowMemoryFlag, allocationBlockSize)
        { }
    }

    public unsafe class ByteStringContext<TAllocator> : IDisposable where TAllocator : struct, IByteStringAllocator
    {
        public static TAllocator Allocator;

        public event Action AllocationFailed;

        private class SegmentInformation
        {
            public readonly UnmanagedGlobalSegment Memory;
            public readonly byte* Start;
            public readonly byte* End;
            public readonly bool CanDispose;

            public byte* Current;

            public SegmentInformation(UnmanagedGlobalSegment memory, byte* start, byte* end, bool canDispose)
            {
                Memory = memory;
                Start = start;
                End = end;

                Current = start;
                CanDispose = canDispose;
            }

            public SegmentInformation(byte* start, byte* end, bool canDispose)
            {
                Start = start;
                End = end;

                Current = start;
                CanDispose = canDispose;
            }

            public int Size
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return (int)(End - Start); }
            }

            public int SizeLeft
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return (int)(End - Current); }
            }
        }

        // Log₂(MinBlockSizeInBytes)
        private const int LogMinBlockSize = 12;

        /// <summary>
        /// This list keeps all the segments already instantiated in order to release them after context finalization. 
        /// </summary>
        private readonly List<SegmentInformation> _wholeSegments;
        private int _allocationBlockSize;

        private long _totalAllocated, _currentlyAllocated;

        /// <summary>
        /// This list keeps the hot segments released for use. It is important to note that we will never put into this list
        /// a segment with less space than the MinBlockSize value.
        /// </summary>
        private readonly List<SegmentInformation> _internalReadyToUseMemorySegments;
        private readonly int[] _internalReusableStringPoolCount;
        private readonly FastStack<IntPtr>[] _internalReusableStringPool;
        private SegmentInformation _internalCurrent;


        private const int ExternalFastPoolSize = 16;
        private int _externalAlignedSize = 0;
        private int _externalCurrentLeft = 0;
        private int _externalFastPoolCount = 0;
        private readonly IntPtr[] _externalFastPool = new IntPtr[ExternalFastPoolSize];
        private readonly FastStack<IntPtr> _externalStringPool;
        private SegmentInformation _externalCurrent;

        public ByteStringContext(SharedMultipleUseFlag lowMemoryFlag, int allocationBlockSize = ByteStringContext.DefaultAllocationBlockSizeInBytes)
        {
            if (allocationBlockSize < ByteStringContext.MinBlockSizeInBytes)
                throw new ArgumentException($"It is not a good idea to allocate chunks of less than the {nameof(ByteStringContext.MinBlockSizeInBytes)} value of {ByteStringContext.MinBlockSizeInBytes}");

            _lowMemoryFlag = lowMemoryFlag;
            _allocationBlockSize = allocationBlockSize;

            _wholeSegments = new List<SegmentInformation>();
            _internalReadyToUseMemorySegments = new List<SegmentInformation>();

            _internalReusableStringPool = new FastStack<IntPtr>[LogMinBlockSize];
            _internalReusableStringPoolCount = new int[LogMinBlockSize];

            _internalCurrent = AllocateSegment(allocationBlockSize);
            AllocateExternalSegment(allocationBlockSize);

            _externalStringPool = new FastStack<IntPtr>(64);

            PrepareForValidation();
        }

        public void Reset()
        {
            if (_disposed)
            {
                if (_isFinalizerThread)
                    return;
                ThrowObjectDisposed();
            }
                

            Array.Clear(_internalReusableStringPoolCount, 0, _internalReusableStringPoolCount.Length);
            foreach (var stack in _internalReusableStringPool)
            {
                stack?.Clear();
            }
            _internalReadyToUseMemorySegments.Clear();// memory here will be released from _wholeSegments

            _externalStringPool.Clear();
            _externalFastPoolCount = 0;
            _externalCurrentLeft = (int)(_externalCurrent.End - _externalCurrent.Start) / _externalAlignedSize;

            Debug.Assert(_wholeSegments.Count >= 2);
            // We need to make ensure that the _internalCurrent is linked to an unmanaged segment
            var index = _wholeSegments.Count - 1;
            if (_wholeSegments[index] == _externalCurrent)
            {
                index = _wholeSegments.Count - 2;
            }
            _internalCurrent = _wholeSegments[index];

            _internalCurrent.Current = _internalCurrent.Start;
            _externalCurrent.Current = _externalCurrent.Start; // no need to reset it, always whole section

            _currentlyAllocated = 0;

            if (_wholeSegments.Count == 2)
                return;

            for (int i = 0; i < _wholeSegments.Count; i++)
            {
                var segment = _wholeSegments[i];
                if (segment == _internalCurrent || segment == _externalCurrent)
                    continue;

                ReleaseSegment(segment);
                _wholeSegments.RemoveAt(i);
                i--;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public InternalScope Allocate(int length, out ByteString output)
        {
            output = AllocateInternal(length, ByteStringType.Mutable);
            return new InternalScope(this, output);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetPoolIndexForReuse(int size)
        {
            return Bits.CeilLog2(size) - 1; // x^0 = 1 therefore we start counting at 1 instead.
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetPoolIndexForReservation(int size)
        {
            return Bits.MostSignificantBit(size) - 1; // x^0 = 1 therefore we start counting at 1 instead.
        }

        public override string ToString()
        {
            return $"Allocated {Sizes.Humane(_currentlyAllocated)} / {Sizes.Humane(_totalAllocated)}";
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ByteString AllocateExternal(byte* valuePtr, int size, ByteStringType type)
        {
            Debug.Assert((type & ByteStringType.External) != 0, "This allocation routine is only for use with external storage byte strings.");

            _currentlyAllocated += _externalAlignedSize;

            ByteStringStorage* storagePtr;
            if (_externalFastPoolCount > 0)
            {
                storagePtr = (ByteStringStorage*)_externalFastPool[--_externalFastPoolCount].ToPointer();
            }
            else if (_externalStringPool.Count != 0)
            {
                storagePtr = (ByteStringStorage*)_externalStringPool.Pop().ToPointer();
            }
            else
            {
                if (_externalCurrentLeft == 0)
                {
                    var tmp = Math.Min(2 * Constants.Size.Megabyte, _allocationBlockSize * 2);
                    AllocateExternalSegment(tmp);
                    _allocationBlockSize = tmp;
                }

                storagePtr = (ByteStringStorage*)_externalCurrent.Current;
                _externalCurrent.Current += _externalAlignedSize;
                _externalCurrentLeft--;
            }

            storagePtr->Flags = type;
            storagePtr->Length = size;
            storagePtr->Ptr = valuePtr;

            // We are registering the storage for validation here. Not the ByteString itself
            RegisterForValidation(storagePtr);

            return new ByteString(storagePtr);
        }

        private ByteString AllocateInternal(int length, ByteStringType type)
        {
            if (_disposed)
                ThrowObjectDisposed();

            Debug.Assert((type & ByteStringType.External) == 0, "This allocation routine is only for use with internal storage byte strings.");
            type &= ~ByteStringType.External; // We are allocating internal, so we will force it (even if we are checking for it in debug).

            int allocationSize = length + sizeof(ByteStringStorage);

           

            // This is even bigger than the configured allocation block size. There is no reason why we shouldn't
            // allocate it directly. When released (if released) this will be reused as a segment, ensuring that the context
            // could handle that.
            if (allocationSize > _allocationBlockSize)
                goto AllocateWhole;

            int reusablePoolIndex = GetPoolIndexForReuse(allocationSize);
            int allocationUnit = Bits.NextPowerOf2(allocationSize);

            // The allocation unit is bigger than MinBlockSize (therefore it wont be 2^n aligned).
            // Then we will 64bits align the allocation.
            if (allocationUnit > ByteStringContext.MinBlockSizeInBytes)
                allocationUnit += sizeof(long) - allocationUnit % sizeof(long);

            // All allocation units are 32 bits aligned. If not we will have a performance issue.
            Debug.Assert(allocationUnit % sizeof(int) == 0);

            _currentlyAllocated += allocationUnit;

            // If we can reuse... we retrieve those.
            if (allocationSize <= ByteStringContext.MinBlockSizeInBytes && _internalReusableStringPoolCount[reusablePoolIndex] != 0)
            {
                // This is a stack because hotter memory will be on top. 
                FastStack<IntPtr> pool = _internalReusableStringPool[reusablePoolIndex];

                _internalReusableStringPoolCount[reusablePoolIndex]--;
                void* ptr = pool.Pop().ToPointer();

                return Create(ptr, length, allocationUnit, type);
            }

            // We will allocate from the current segment.
            if (allocationUnit <= _internalCurrent.SizeLeft)
            {
                var byteString = Create(_internalCurrent.Current, length, allocationUnit, type);
                _internalCurrent.Current += byteString._pointer->Size;

                return byteString;
            }

            return AllocateInternalUnlikely(length, allocationUnit, type); // We will allocate also allocating a segment. 

            AllocateWhole:
            return AllocateWholeSegment(length, type); // We will pass the length because this is a whole allocated segment able to hold a length size ByteString.
        }

        private ByteString AllocateInternalUnlikely(int length, int allocationUnit, ByteStringType type)
        {
            SegmentInformation segment = null;

            // We will try to find a hot segment with enough space if available.
            // Older (colder) segments are at the front of the list. That's why we would start scanning backwards.
            for (int i = _internalReadyToUseMemorySegments.Count - 1; i >= 0; i--)
            {
                var segmentValue = _internalReadyToUseMemorySegments[i];
                if (segmentValue.SizeLeft >= allocationUnit)
                {
                    // Put the last where this one is (if it is the same, this is a no-op) and remove it from the list.
                    _internalReadyToUseMemorySegments[i] = _internalReadyToUseMemorySegments[_internalReadyToUseMemorySegments.Count - 1];
                    _internalReadyToUseMemorySegments.RemoveAt(_internalReadyToUseMemorySegments.Count - 1);

                    segment = segmentValue;
                    break;
                }
            }

            // If the size left is bigger than MinBlockSize, we release current as a reusable segment
            int currentSizeLeft = _internalCurrent.SizeLeft;
            if (currentSizeLeft > ByteStringContext.MinBlockSizeInBytes)
            {
                byte* start = _internalCurrent.Current;
                byte* end = start + currentSizeLeft;

                _internalReadyToUseMemorySegments.Add(new SegmentInformation(start, end, false));
            }
            else if (currentSizeLeft > sizeof(ByteStringType) + ByteStringContext.MinReusableBlockSizeInBytes)
            {
                // The memory chunk left is big enough to make sense to reuse it.
                int reusablePoolIndex = GetPoolIndexForReservation(currentSizeLeft);

                FastStack<IntPtr> pool = _internalReusableStringPool[reusablePoolIndex];
                if (pool == null)
                {
                    pool = new FastStack<IntPtr>();
                    _internalReusableStringPool[reusablePoolIndex] = pool;
                }

                pool.Push(new IntPtr(_internalCurrent.Current));
                _internalReusableStringPoolCount[reusablePoolIndex]++;
            }

            // Use the segment and if there is no segment available that matches the request, just get a new one.
            if (segment != null)
            {
                _internalCurrent = segment;
            }
            else
            {
                _allocationBlockSize = Math.Min(2 * Constants.Size.Megabyte, _allocationBlockSize * 2);
                _internalCurrent = AllocateSegment(_allocationBlockSize);
            }

            var byteString = Create(_internalCurrent.Current, length, allocationUnit, type);
            _internalCurrent.Current += byteString._pointer->Size;

            return byteString;
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
                ToLowerTempBuffer = new char[Bits.NextPowerOf2(charCount)];
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
        private ByteString Create(void* ptr, int length, int size, ByteStringType type = ByteStringType.Immutable)
        {
            Debug.Assert(length <= size - sizeof(ByteStringStorage));

            var basePtr = (ByteStringStorage*)ptr;
            basePtr->Flags = type;
            basePtr->Length = length;
            basePtr->Ptr = (byte*)ptr + sizeof(ByteStringStorage);
            basePtr->Size = size;

            // We are registering the storage for validation here. Not the ByteString itself
            RegisterForValidation(basePtr);

            return new ByteString(basePtr);
        }

        private ByteString AllocateWholeSegment(int length, ByteStringType type)
        {
            var size = Bits.NextPowerOf2(length + sizeof(ByteStringStorage));
            SegmentInformation segment = AllocateSegment(size);

            var byteString = Create(segment.Current, length, segment.Size, type);
            segment.Current += byteString._pointer->Size;
            _currentlyAllocated += byteString._pointer->Size;

            return byteString;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReleaseExternal(ref ByteString value)
        {
            if (_disposed)
                ThrowObjectDisposed();

            Debug.Assert(value._pointer != null, "Pointer cannot be null. You have a defect in your code.");

            if (value._pointer == null) // this is a safe-guard on Release, it is better to not release the memory than fail
                return;

            _currentlyAllocated -= _externalAlignedSize;

            Debug.Assert(value.IsExternal, "Cannot release as external an internal pointer.");

            // We are releasing, therefore we should validate among other things if an immutable string changed and if we are the owners.
            ValidateAndUnregister(value);

            value._pointer->Flags = ByteStringType.Disposed;

            // We release the pointer in the appropriate reuse pool.
            if (_externalFastPoolCount < ExternalFastPoolSize)
            {
                // Release in the fast pool. 
                _externalFastPool[_externalFastPoolCount++] = new IntPtr(value._pointer);
            }
            else
            {
                _externalStringPool.Push(new IntPtr(value._pointer));
            }

#if VALIDATE
            // Setting the null key ensures that in between we can validate that no further deallocation
            // happens on this memory segment.
            value._pointer->Key = ByteStringStorage.NullKey;

            // Setting the length to zero ensures that the hash returns 0 and do not 
            // fail with an AccessViolationException because there is garbage stored here.
            value._pointer->Length = 0;
#endif

            // WE WANT it to happen, no matter what. 
            value._pointer = null;
        }

        public void Release(ref ByteString value)
        {
            if (_disposed)
                ThrowObjectDisposed();

            Debug.Assert(value._pointer != null, "Pointer cannot be null. You have a defect in your code.");
            if (value._pointer == null) // this is a safe-guard on Release, it is better to not release the memory than fail
                return;
            Debug.Assert(value._pointer->Flags != ByteStringType.Disposed, "Double free");
            Debug.Assert(!value.IsExternal, "Cannot release as internal an external pointer.");

            _currentlyAllocated -= value._pointer->Size;

            // We are releasing, therefore we should validate among other things if an immutable string changed and if we are the owners.
            ValidateAndUnregister(value);

            int reusablePoolIndex = GetPoolIndexForReuse(value._pointer->Size);

            if (value._pointer->Size <= ByteStringContext.MinBlockSizeInBytes)
            {
                FastStack<IntPtr> pool = _internalReusableStringPool[reusablePoolIndex];
                if (pool == null)
                {
                    pool = new FastStack<IntPtr>();
                    _internalReusableStringPool[reusablePoolIndex] = pool;
                }

                pool.Push(new IntPtr(value._pointer));
                _internalReusableStringPoolCount[reusablePoolIndex]++;
            }
            else  // The released memory is big enough, we will just release it as a new segment. 
            {
                byte* start = (byte*)value._pointer;
                byte* end = start + value._pointer->Size;

                // Given that this is put into a reuse queue, we are not providing the Segment because it has no ownership of it.
                var segment = new SegmentInformation (start, end, false );
                _internalReadyToUseMemorySegments.Add(segment);
            }

#if VALIDATE
            // Setting the null key ensures that in between we can validate that no further deallocation
            // happens on this memory segment.
            value._pointer->Key = ByteStringStorage.NullKey;

            // Setting the length to zero ensures that the hash returns 0 and do not 
            // fail with an AccessViolationException because there is garbage stored here.
            value._pointer->Length = 0;
#endif

            // WE WANT it to happen, no matter what. 
            value._pointer = null;
        }

        private SegmentInformation AllocateSegment(int size)
        {
            var memorySegment = Allocator.Allocate(size, AllocationFailed);
            if(memorySegment.Segment == null)
                ThrowInvalidMemorySegmentOnAllocation();

            _totalAllocated += memorySegment.Size;

            byte* start = memorySegment.Segment;
            byte* end = start + memorySegment.Size;

            var segment = new SegmentInformation( memorySegment, start, end, true );
            _wholeSegments.Add(segment);

            return segment;
        }

        private void ThrowInvalidMemorySegmentOnAllocation()
        {
            AllocationFailed?.Invoke();
            throw new InvalidOperationException("Allocate gave us a segment that was already disposed.");
        }

        private void ThrowObjectDisposed()
        {
            AllocationFailed?.Invoke();
            throw new ObjectDisposedException("ByteStringContext");
        }

        private void AllocateExternalSegment(int size)
        {
            var memorySegment = Allocator.Allocate(size, AllocationFailed);

            _totalAllocated += memorySegment.Size;

            byte* start = memorySegment.Segment;
            byte* end = start + memorySegment.Size;

            _externalCurrent = new SegmentInformation ( memorySegment, start, end, true );
            _externalAlignedSize = (sizeof(ByteStringStorage) + (sizeof(long) - sizeof(ByteStringStorage) % sizeof(long)));
            _externalCurrentLeft = (int)(_externalCurrent.End - _externalCurrent.Start) / _externalAlignedSize;

            _wholeSegments.Add(_externalCurrent);
        }

        public ByteString Skip(ByteString value, int bytesToSkip, ByteStringType type = ByteStringType.Mutable)
        {
            Debug.Assert(value._pointer != null, "ByteString cant be null.");

            if (_disposed)
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
            private ByteStringContext<TAllocator> _parent;
            private ByteString _str;

            public Scope(ByteStringContext<TAllocator> parent, ByteString str)
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
            private ByteStringContext<TAllocator> _parent;
            private ByteString _str;

            public InternalScope(ByteStringContext<TAllocator> parent, ByteString str)
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
            private ByteStringContext<TAllocator> _parent;
            private ByteString _str;

            public ExternalScope(ByteStringContext<TAllocator> parent, ByteString str)
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

        private bool _disposed;

        public void Dispose()
        {
            lock (this)
            {
                if (_disposed)
                    return;

                GC.SuppressFinalize(this);

                _disposed = true;


                foreach (var segment in _wholeSegments)
                {
                    ReleaseSegment(segment);
                }

                _wholeSegments.Clear();
                _internalReadyToUseMemorySegments.Clear();
            }
        }

        [ThreadStatic] private static bool _isFinalizerThread;
        private readonly SharedMultipleUseFlag _lowMemoryFlag;

        private void ReleaseSegment(SegmentInformation segment)
        {
            if (!segment.CanDispose)
                return;

            _totalAllocated -= segment.Size;

            // Check if we can release this memory segment back to the pool.
            if (_isFinalizerThread || 
                segment.Memory.Size > ByteStringContext.MaxAllocationBlockSizeInBytes ||
                _lowMemoryFlag)
            {
                segment.Memory.Dispose();
            }
            else
            {
                Allocator.Free(segment.Memory);
            }
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
