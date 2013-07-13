using System;
using System.Runtime.InteropServices;

namespace Nevar
{
	internal unsafe class NativeMethods
	{
		[DllImport("msvcrt.dll", EntryPoint = "memcpy", CallingConvention = CallingConvention.Cdecl, SetLastError = false)]
		public static extern IntPtr MemCpy(byte* dest, byte* src, int count); 
	}
}