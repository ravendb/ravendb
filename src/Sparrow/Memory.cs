using System.Runtime.CompilerServices;
using Sparrow.Platform;
using Sparrow.Platform.Posix;

namespace Sparrow
{
    public static unsafe class Memory
    {
        private const int CompareInlineVsCallThreshold = 256;

        public static int Compare(byte* p1, byte* p2, int size)
        {
            return CompareInline(p1, p2, size);
        }

        public static int Compare(byte* p1, byte* p2, int size, out int position)
        {
            return CompareInline(p1, p2, size, out position);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CompareInline(void* p1, void* p2, int size)
        {
            // If we use an unmanaged bulk version with an inline compare the caller site does not get optimized properly.
            // If you know you will be comparing big memory chunks do not use the inline version. 
            int l = size;
            if (l > CompareInlineVsCallThreshold)
                goto UnmanagedCompare;

            byte* bpx = (byte*)p1, bpy = (byte*)p2;
            int last;
            for (int i = 0; i < l / 8; i++, bpx += 8, bpy += 8)
            {
                if (*((long*)bpx) != *((long*)bpy))
                {
                    last = 8;
                    goto Tail;
                }
            }

            if ((l & 4) != 0)
            {
                if (*((int*)bpx) != *((int*)bpy))
                {
                    last = 4;
                    goto Tail;
                }
                bpx += 4;
                bpy += 4;
            }

            if ((l & 2) != 0)
            {
                if (*((short*)bpx) != *((short*)bpy))
                {
                    last = 2;
                    goto Tail;
                }

                bpx += 2;
                bpy += 2;
            }

            if ((l & 1) != 0)
            {
                return (*((byte*)bpx) - *((byte*)bpy));
            }

            return 0;

        Tail:
            while (last > 0)
            {
                if (*((byte*)bpx) != *((byte*)bpy))
                    return *bpx - *bpy;

                bpx++;
                bpy++;
                last--;
            }

            return 0;

        UnmanagedCompare:
            return PlatformDetails.RunningOnPosix
                ? Syscall.Compare((byte*)p1, (byte*)p2, size)
                : Win32UnmanagedMemory.Compare((byte*)p1, (byte*)p2, size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CompareInline(byte* p1, byte* p2, int size, out int position)
        {
            byte* bpx = p1, bpy = p2;
            int l = size;

            int last = 0;
            for (int i = 0; i < l / 8; i++, bpx += 8, bpy += 8)
            {
                if (*((long*)bpx) != *((long*)bpy))
                {
                    last = 8;
                    goto TAIL;
                }
            }

            if ((l & 4) != 0)
            {
                if (*((int*)bpx) != *((int*)bpy))
                {
                    last = 4;
                    goto TAIL;
                }
                bpx += 4;
                bpy += 4;
            }
            if ((l & 2) != 0)
            {
                if (*((short*)bpx) != *((short*)bpy))
                {
                    last = 2;
                    goto TAIL;
                }

                bpx += 2;
                bpy += 2;
            }

            if ((l & 1) != 0)
            {
                position = (int)(bpx - p1);
                return (*((byte*)bpx) - *((byte*)bpy));
            }

            position = size;
            return 0;

        TAIL:
            while (last > 0)
            {
                if (*((byte*)bpx) != *((byte*)bpy))
                {
                    position = (int)(bpx - p1);
                    return *bpx - *bpy;
                }

                bpx++;
                bpy++;
                last--;
            }

            position = size;
            return 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Copy(void* dest, void* src, uint n)
        {
            Unsafe.CopyBlock(dest, src, n);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Copy(void* dest, void* src, long n)
        {
            if (n < uint.MaxValue) // Common code-path
            {
                Copy(dest, src, (uint)n);
                return;
            }
            
            CopyLong(dest, src, n);
        }

        private static void CopyLong(void* dest, void* src, long n)
        {
            for (long i = 0; i < n; i += uint.MaxValue)
            {
                var size = uint.MaxValue;
                if (i + uint.MaxValue > n)
                    size = (uint)(n % uint.MaxValue);
                Copy((byte*)dest + i, (byte*)src + i, size);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Set(byte* dest, byte value, uint n)
        {
            Unsafe.InitBlock(dest, value, n);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Set(byte* dest, byte value, long n)
        {
            if (n < uint.MaxValue) // Common code-path
            {
                Set(dest, value, (uint)n);
                return;
            }
            
            SetLong(dest, value, n);
        }

        private static void SetLong(byte* dest, byte value, long n)
        {
            for (long i = 0; i < n; i += uint.MaxValue)
            {
                var size = uint.MaxValue;
                if (i + uint.MaxValue > n)
                    size = (uint)(n % uint.MaxValue);
                Set(dest + i, value, size);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Move(byte* dest, byte* src, int n)
        {
            // if dest and src overlaps, we need to call specifically to memmove pinvoke supporting overlapping
            if (dest + n >= src &&
                src + n >= dest)
            {
                var _ = PlatformDetails.RunningOnPosix
                    ? Syscall.Move(dest, src, n)
                    : Win32UnmanagedMemory.Move(dest, src, n);
                return;
            }

            Copy(dest, src, n);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static TTo As<TFrom, TTo>(ref TFrom value)
        {
            return Unsafe.As<TFrom, TTo>(ref value);            
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T Read<T>(byte* ptr)
        {
            return Unsafe.Read<T>(ptr);
        }
    }
}
