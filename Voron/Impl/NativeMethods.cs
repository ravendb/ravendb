using System;
using System.Runtime.InteropServices;

namespace Voron.Impl
{
	public static unsafe class NativeMethods
	{
		[DllImport("msvcrt.dll", EntryPoint = "memcpy", CallingConvention = CallingConvention.Cdecl, SetLastError = false)]
        public static extern IntPtr memcpy(byte* dest, byte* src, IntPtr count);

        [DllImport("msvcrt.dll", EntryPoint = "memcpy", CallingConvention = CallingConvention.Cdecl, SetLastError = false)]
        public static extern IntPtr memcpy(byte* dest, byte* src, int count);

		[DllImport("msvcrt.dll", CallingConvention = CallingConvention.Cdecl, SetLastError = false)]
		public static extern int memcmp(byte* b1, byte* b2, int count);

		[DllImport("msvcrt.dll", CallingConvention = CallingConvention.Cdecl, SetLastError = false)]
		public static extern int memmove(byte* b1, byte* b2, int count);

        [DllImport("msvcrt.dll", EntryPoint = "memset", CallingConvention = CallingConvention.Cdecl, SetLastError = false)]
        public static extern IntPtr memset(byte* dest, int c, int count);
	}

	public static unsafe class Win32NativeMethods
	{
		[DllImport("kernel32.dll", SetLastError = true)]
		public static extern byte* VirtualAlloc(IntPtr lpAddress, UIntPtr dwSize,
		   AllocationType flAllocationType, MemoryProtection flProtect);

		[DllImport("kernel32.dll", SetLastError = true)]
		public static extern bool VirtualFree(byte* lpAddress, UIntPtr dwSize,
		   FreeType dwFreeType);

		[Flags]
		public enum FreeType : uint
		{
			MEM_DECOMMIT = 0x4000,
			MEM_RELEASE = 0x8000
		}

		[Flags]
		public enum AllocationType : uint
		{
			COMMIT = 0x1000,
			RESERVE = 0x2000,
			RESET = 0x80000,
			LARGE_PAGES = 0x20000000,
			PHYSICAL = 0x400000,
			TOP_DOWN = 0x100000,
			WRITE_WATCH = 0x200000
		}

		[Flags]
		public enum MemoryProtection : uint
		{
			EXECUTE = 0x10,
			EXECUTE_READ = 0x20,
			EXECUTE_READWRITE = 0x40,
			EXECUTE_WRITECOPY = 0x80,
			NOACCESS = 0x01,
			READONLY = 0x02,
			READWRITE = 0x04,
			WRITECOPY = 0x08,
			GUARD_Modifierflag = 0x100,
			NOCACHE_Modifierflag = 0x200,
			WRITECOMBINE_Modifierflag = 0x400
		}
	}
}