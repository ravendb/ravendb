using System;
using System.Runtime.CompilerServices;
using Sparrow.LowMemory;
using Sparrow.Threading;

namespace Sparrow
{
    public interface IAllocator {}

    public interface IAllocatorOptions { }

    public interface IFixedSizeAllocatorOptions
    {
        int BlockSize { get; }
    }

    public interface ILifecycleHandler<TAllocator> where TAllocator : struct, IAllocator
    {
        void BeforeInitialize(ref TAllocator allocator);
        void AfterInitialize(ref TAllocator allocator);
        void BeforeDispose(ref TAllocator allocator);
        void BeforeFinalization(ref TAllocator allocator);
    }

    public interface ILowMemoryHandler<TAllocator> where TAllocator : struct, IAllocator
    {
        void NotifyLowMemory(ref TAllocator allocator);
        void NotifyLowMemoryOver(ref TAllocator allocator);
    }

    public interface IRenewable<TAllocator> where TAllocator : struct, IAllocator
    {
        void Renew(ref TAllocator allocator);
    }

    public interface IComposableAllocator<TPointerType> where TPointerType : struct, IPointerType
    {
        /// <summary>
        /// Returns true if the parent allocator will have to take care of the memory requested to the
        /// underlying allocator. Whenever the allocator would ignore this property, it has to default
        /// to true and handle the release process appropriately.
        /// </summary>
        bool HasOwnership { get; }

        IAllocatorComposer<TPointerType> CreateAllocator();
        void ReleaseAllocator(IAllocatorComposer<Pointer> allocator, bool disposing);
    }

    public interface IAllocatorComposer<TPointerType> where TPointerType : struct, IPointerType
    {
        void Initialize<TAllocatorOptions>(TAllocatorOptions options) where TAllocatorOptions : struct, IAllocatorOptions;

        TPointerType Allocate(int size);
        void Release(ref TPointerType ptr);

        void Renew();
        void Reset();
        void Dispose(bool disposing);

        void LowMemory();
        void LowMemoryOver();
    }

    public interface IAllocator<T, TPointerType> : IAllocator
        where T : struct, IAllocator
        where TPointerType : struct, IPointerType
    {
        /// <summary>
        /// This is the total ammount of memory that has been allocated since the last Reset cycle.
        /// </summary>
        long TotalAllocated { get; }

        /// <summary>
        /// This is the total ammount of memory currently either InUse or on hold on internal buffers since the last Reset Cycle.
        /// </summary>
        long Allocated { get; }

        /// <summary>
        /// This the is actual ammount of memory that is on the customer hands at the current moment.
        /// </summary>
        long InUse { get; }

        void Initialize(ref T allocator);

        void Configure<TConfig>(ref T allocator, ref TConfig configuration) where TConfig : struct, IAllocatorOptions;

        TPointerType Allocate(ref T allocator, int size);
        void Release(ref T allocator, ref TPointerType ptr);
        void Reset(ref T allocator);

        void OnAllocate(ref T allocator, TPointerType ptr);
        void OnRelease(ref T allocator, TPointerType ptr);

        void Dispose(ref T allocator, bool disposing);
    }

    public static class Allocator
    {
        public static readonly SharedMultipleUseFlag LowMemoryFlag = new SharedMultipleUseFlag();
    }

    public sealed class Allocator<TAllocator> : IAllocatorComposer<Pointer>, IDisposable, ILowMemoryHandler
        where TAllocator : struct, IAllocator<TAllocator, Pointer>
    {
        private TAllocator _allocator;
        private readonly SingleUseFlag _disposeFlag = new SingleUseFlag();

#if VALIDATE
        private uint _generation;        
#endif

        ~Allocator()
        {
            if (_allocator is ILifecycleHandler<TAllocator> a)
                a.BeforeFinalization(ref _allocator);

            // We are not going to dispose twice, even if we hit here. 
            if (_disposeFlag.Raise())
                Dispose(false);
        }

        public void Initialize<TAllocatorOptions>(TAllocatorOptions options)
            where TAllocatorOptions : struct, IAllocatorOptions
        {

            if (_allocator is ILifecycleHandler<TAllocator> a)
                a.BeforeInitialize(ref _allocator);

            _allocator.Initialize(ref _allocator);
            _allocator.Configure(ref _allocator, ref options);

#if VALIDATE
            this._generation =  (uint)new Random().Next() & 0xFFF00000;
#endif

            if (_allocator is ILowMemoryHandler<TAllocator>)
                LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);

            if (_allocator is ILifecycleHandler<TAllocator> b)
                b.AfterInitialize(ref _allocator);
        }

        /// <summary>
        /// This is the total amount of memory that has been allocated since the last Reset cycle.
        /// </summary>
        public long TotalAllocated
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _allocator.TotalAllocated; }
        }

        /// <summary>
        /// This is the total ammount of memory currently either InUse or on hold on internal buffers since the last Reset Cycle.
        /// </summary>
        public long Allocated
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _allocator.Allocated; }
        }

        /// <summary>
        /// This the is actual ammount of memory that is on the customer hands at the current moment.
        /// </summary>
        public long InUse
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _allocator.InUse; }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Pointer Allocate(int size)
        {
            unsafe
            {
                var ptr = _allocator.Allocate(ref _allocator, size);
                if (_allocator is ILifecycleHandler<TAllocator> a)
                    a.BeforeInitialize(ref _allocator);

#if VALIDATE
                ptr.Generation = this._generation;
#endif

                return ptr;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Pointer<TType> Allocate<TType>(int size) where TType : struct
        {
            unsafe
            {
                var ptr = _allocator.Allocate(ref _allocator, size * Unsafe.SizeOf<TType>());

                // PERF: We cannot make this conditional because the runtime cost would kill us (too much traffic).
                //       But we can call it anyways and use the capability of evicting the call if empty.
                _allocator.OnAllocate(ref _allocator, ptr);

#if VALIDATE
                ptr.Generation = this._generation;
#endif

                return new Pointer<TType>(ptr);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Release<TType>(ref Pointer<TType> ptr) where TType : struct
        {
            unsafe
            {
#if VALIDATE
                // We are not able to validate pointers that are either composed and/or created by the user.
                if (ptr.Generation != 0 && ptr.Generation != this._generation)
                {
                    throw new InvalidOperationException($"The pointer is from generation {ptr.Generation:X} but current generation is {this._generation:X}");
                }
#endif

                // PERF: We cannot make this conditional because the runtime cost would kill us (too much traffic).
                //       But we can call it anyways and use the capability of evicting the call if empty.
                _allocator.OnRelease(ref _allocator, ptr);

                Pointer localPtr = ptr;
                _allocator.Release(ref _allocator, ref localPtr);

                ptr = new Pointer<TType>();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Release(ref Pointer ptr)
        {
            unsafe
            {
#if VALIDATE
                // We are not able to validate pointers that are either composed and/or created by the user.
                if (ptr.Generation != 0 && ptr.Generation != this._generation)
                {
                    throw new InvalidOperationException($"The pointer is from generation {ptr.Generation:X} but current generation is {this._generation:X}");
                }
#endif

                // PERF: We cannot make this conditional because the runtime cost would kill us (too much traffic).
                //       But we can call it anyways and use the capability of evicting the call if empty.
                _allocator.OnRelease(ref _allocator, ptr);

                _allocator.Release(ref _allocator, ref ptr);

                ptr = new Pointer();
            }
        }

        public void Renew()
        {
            if (_allocator is IRenewable<TAllocator> a)
                a.Renew(ref _allocator);

#if VALIDATE
            this._generation++;
#endif
        }

        public void Reset()
        {
            _allocator.Reset(ref _allocator);

#if VALIDATE
            this._generation++;
#endif

        }

        public void Dispose()
        {
            Dispose(true);
        }

        public void Dispose(bool disposing)
        {
            if (_disposeFlag.Raise())
                _allocator.Dispose(ref _allocator, disposing);

            GC.SuppressFinalize(this);
        }

        public void LowMemory()
        {
            if (_allocator is ILowMemoryHandler<TAllocator> a)
                a.NotifyLowMemory(ref _allocator);
        }

        public void LowMemoryOver()
        {
            if (_allocator is ILowMemoryHandler<TAllocator> a)
                a.NotifyLowMemoryOver(ref _allocator);
        }
    }

    public sealed class BlockAllocator<TAllocator> : IAllocatorComposer<BlockPointer>, IDisposable, ILowMemoryHandler
        where TAllocator : struct, IAllocator<TAllocator, BlockPointer>, IAllocator
    {
        private TAllocator _allocator;
        private readonly SingleUseFlag _disposeFlag = new SingleUseFlag();

#if VALIDATE
        private uint _generation;        
#endif

        ~BlockAllocator()
        {
            if (_allocator is ILifecycleHandler<TAllocator> a)
                a.BeforeFinalization(ref _allocator);

            // We are not going to dispose twice, even if we hit here. 
            if (_disposeFlag.Raise())
                Dispose(false);
        }

        public void Initialize<TAllocatorOptions>(TAllocatorOptions options)
            where TAllocatorOptions : struct, IAllocatorOptions
        {

            if (_allocator is ILifecycleHandler<TAllocator> a)
                a.BeforeInitialize(ref _allocator);

            _allocator.Initialize(ref _allocator);
            _allocator.Configure(ref _allocator, ref options);

#if VALIDATE
            this._generation = (uint)new Random().Next() & 0xFFF00000;
#endif

            if (_allocator is ILowMemoryHandler<TAllocator>)
                LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);

            if (_allocator is ILifecycleHandler<TAllocator> b)
                b.AfterInitialize(ref _allocator);
        }

        /// <summary>
        /// This is the total ammount of memory that has been allocated since the last Reset cycle.
        /// </summary>
        public long TotalAllocated
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _allocator.TotalAllocated; }
        }

        /// <summary>
        /// This is the total ammount of memory currently either InUse or on hold on internal buffers since the last Reset Cycle.
        /// </summary>
        public long Allocated
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _allocator.Allocated; }
        }

        /// <summary>
        /// This the is actual ammount of memory that is on the customer hands at the current moment.
        /// </summary>
        public long InUse
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _allocator.InUse; }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BlockPointer Allocate(int size)
        {
            unsafe
            {
                var ptr = _allocator.Allocate(ref _allocator, size);
                if (_allocator is ILifecycleHandler<TAllocator> a)
                    a.BeforeInitialize(ref _allocator);

#if VALIDATE
                ptr.Generation = this._generation;
#endif

                return ptr;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BlockPointer<TType> Allocate<TType>(int size) where TType : struct
        {
            unsafe
            {
                var ptr = _allocator.Allocate(ref _allocator, size * Unsafe.SizeOf<TType>());

                // PERF: We cannot make this conditional because the runtime cost would kill us (too much traffic).
                //       But we can call it anyways and use the capability of evicting the call if empty.
                _allocator.OnAllocate(ref _allocator, ptr);

#if VALIDATE
                ptr.Generation = this._generation;
#endif

                return new BlockPointer<TType>(ptr);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Release<TType>(ref BlockPointer<TType> ptr) where TType : struct
        {
            unsafe
            {
#if VALIDATE
                // We are not able to validate pointers that are either composed and/or created by the user.
                if (ptr.Generation != 0 && ptr.Generation != this._generation)
                {
                    throw new InvalidOperationException($"The pointer is from generation {ptr.Generation:X} but current generation is {this._generation:X}");
                }
#endif

                // PERF: We cannot make this conditional because the runtime cost would kill us (too much traffic).
                //       But we can call it anyways and use the capability of evicting the call if empty.
                _allocator.OnRelease(ref _allocator, ptr);

                BlockPointer localPtr = ptr;
                _allocator.Release(ref _allocator, ref localPtr);

                ptr = new BlockPointer<TType>();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Release(ref BlockPointer ptr)
        {
            unsafe
            {
#if VALIDATE
                // We are not able to validate pointers that are either composed and/or created by the user.
                if (ptr.Generation != 0 && ptr.Generation != this._generation)
                {
                    throw new InvalidOperationException($"The pointer is from generation {ptr.Generation:X} but current generation is {this._generation:X}");
                }
#endif

                // PERF: We cannot make this conditional because the runtime cost would kill us (too much traffic).
                //       But we can call it anyways and use the capability of evicting the call if empty.
                _allocator.OnRelease(ref _allocator, ptr);

                _allocator.Release(ref _allocator, ref ptr);

                ptr = new BlockPointer();
            }
        }

        public void Renew()
        {
            if (_allocator is IRenewable<TAllocator> a)
                a.Renew(ref _allocator);

#if VALIDATE
            this._generation++;
#endif
        }

        public void Reset()
        {
            _allocator.Reset(ref _allocator);

#if VALIDATE
            this._generation++;
#endif
        }

        public void Dispose()
        {
            Dispose(true);
        }

        public void Dispose(bool disposing)
        {
            if (_disposeFlag.Raise())
                _allocator.Dispose(ref _allocator, disposing);

            GC.SuppressFinalize(this);
        }

        public void LowMemory()
        {
            if (_allocator is ILowMemoryHandler<TAllocator> a)
                a.NotifyLowMemory(ref _allocator);
        }

        public void LowMemoryOver()
        {
            if (_allocator is ILowMemoryHandler<TAllocator> a)
                a.NotifyLowMemoryOver(ref _allocator);
        }
    }

    public sealed class FixedSizeAllocator<TAllocator> : IDisposable, ILowMemoryHandler
        where TAllocator : struct, IAllocator<TAllocator, Pointer>, IAllocator
    {
        private int _blockSize;
        private TAllocator _allocator;
        private readonly SingleUseFlag _disposeFlag = new SingleUseFlag();

#if VALIDATE
        private uint _generation;        
#endif

        ~FixedSizeAllocator()
        {
            if (_allocator is ILifecycleHandler<TAllocator> a)
                a.BeforeFinalization(ref _allocator);

            // We are not going to dispose twice, even if we hit here. 
            if (_disposeFlag.Raise())
                _allocator.Dispose(ref _allocator, false);
        }

        public void Initialize<TConfig>(TConfig options)
            where TConfig : struct, IAllocatorOptions
        {

            if (!typeof(IFixedSizeAllocatorOptions).IsAssignableFrom(typeof(TConfig)))
                throw new NotSupportedException($"{nameof(TConfig)} is not compatible with {nameof(TConfig)}");

            this._blockSize = ((IFixedSizeAllocatorOptions)options).BlockSize;

            if (_allocator is ILifecycleHandler<TAllocator> a)
                a.BeforeInitialize(ref _allocator);

            _allocator.Initialize(ref _allocator);
            _allocator.Configure(ref _allocator, ref options);

#if VALIDATE
            this._generation = (uint)new Random().Next() & 0xFFF00000;
#endif

            if (_allocator is ILowMemoryHandler<TAllocator>)
                LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);

            if (_allocator is ILifecycleHandler<TAllocator> b)
                b.AfterInitialize(ref _allocator);
        }

        public long Allocated
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _allocator.TotalAllocated; }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Pointer Allocate()
        {
            unsafe
            {
                var ptr = _allocator.Allocate(ref _allocator, this._blockSize);
                if (_allocator is ILifecycleHandler<TAllocator> a)
                    a.BeforeInitialize(ref _allocator);

#if VALIDATE
                ptr.Generation = this._generation;
#endif

                return ptr;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Pointer<TType> Allocate<TType>() where TType : struct
        {
            unsafe
            {
                var ptr = _allocator.Allocate(ref _allocator, this._blockSize * Unsafe.SizeOf<TType>());

                // PERF: We cannot make this conditional because the runtime cost would kill us (too much traffic).
                //       But we can call it anyways and use the capability of evicting the call if empty.
                _allocator.OnAllocate(ref _allocator, ptr);

#if VALIDATE
                ptr.Generation = this._generation;
#endif

                return new Pointer<TType>(ptr);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Release<TType>(ref Pointer<TType> ptr) where TType : struct
        {
            unsafe
            {
#if VALIDATE
                // We are not able to validate pointers that are either composed and/or created by the user.
                if (ptr.Generation != 0 && ptr.Generation != this._generation)
                {
                    throw new InvalidOperationException($"The pointer is from generation {ptr.Generation:X} but current generation is {this._generation:X}");
                }
#endif

                // PERF: We cannot make this conditional because the runtime cost would kill us (too much traffic).
                //       But we can call it anyways and use the capability of evicting the call if empty.
                _allocator.OnRelease(ref _allocator, ptr);

                Pointer localPtr = ptr;
                _allocator.Release(ref _allocator, ref localPtr);

                ptr = new Pointer<TType>();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Release(ref Pointer ptr)
        {
            unsafe
            {
#if VALIDATE
                // We are not able to validate pointers that are either composed and/or created by the user.
                if (ptr.Generation != 0 && ptr.Generation != this._generation)
                {
                    throw new InvalidOperationException($"The pointer is from generation {ptr.Generation:X} but current generation is {this._generation:X}");
                }
#endif

                // PERF: We cannot make this conditional because the runtime cost would kill us (too much traffic).
                //       But we can call it anyways and use the capability of evicting the call if empty.
                _allocator.OnRelease(ref _allocator, ptr);

                _allocator.Release(ref _allocator, ref ptr);

                ptr = new Pointer();
            }
        }

        public void Renew()
        {
            if (_allocator is IRenewable<TAllocator> a)
                a.Renew(ref _allocator);

#if VALIDATE
            this._generation++;
#endif
        }

        public void Reset()
        {
            _allocator.Reset(ref _allocator);

#if VALIDATE
            this._generation++;
#endif
        }

        public void Dispose()
        {
            if (_disposeFlag.Raise())
                _allocator.Dispose(ref _allocator, true);

            GC.SuppressFinalize(this);
        }

        public void LowMemory()
        {
            if (_allocator is ILowMemoryHandler<TAllocator> a)
                a.NotifyLowMemory(ref _allocator);
        }
        
        public void LowMemoryOver()
        {
            if (_allocator is ILowMemoryHandler<TAllocator> a)
                a.NotifyLowMemoryOver(ref _allocator);
        }
    }
}
