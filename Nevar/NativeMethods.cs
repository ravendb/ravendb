using System;
using System.Runtime.InteropServices;

namespace Nevar
{
	public unsafe class NativeMethods
	{
		[DllImport("msvcrt.dll", EntryPoint = "memcpy", CallingConvention = CallingConvention.Cdecl, SetLastError = false)]
		public static extern IntPtr memcpy(byte* dest, byte* src, int count);

		[DllImport("msvcrt.dll", CallingConvention = CallingConvention.Cdecl, SetLastError = false)]
		public static extern int memcmp(byte* b1, byte* b2, int count);
	}

}