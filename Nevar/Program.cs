using System;
using System.ComponentModel;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using System.Security;
using System.Text;
using Microsoft.Win32.SafeHandles;

namespace Nevar
{
	public unsafe class Program
	{
		[DllImport("msvcrt.dll", CallingConvention = CallingConvention.Cdecl)]
		static extern int memcmp(byte* b1, byte* b2, int count);

		static unsafe void Main(string[] args)
		{
			var mmf = MemoryMappedFile.CreateNew("test", 1024 * 16);
			var pager = new Pager(mmf);
			var tx = new Transaction
				{
					Pager = pager,
					NextPageNumber = 0
				};

			var tree = Tree.CreateOrOpen(tx, -1, memcmp);
			var ms = new MemoryStream(Encoding.UTF8.GetBytes("hi there"));
			tree.Add(tx, "test", ms);
		}
	}
}

