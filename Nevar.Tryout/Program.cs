using System;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Text;
using Nevar.Debugging;
using Nevar.Impl;
using Nevar.Impl.FileHeaders;
using Nevar.Tests.Trees;

namespace Nevar.Tryout
{
	unsafe class Program
	{
		static void Main(string[] args)
		{
            //new FreeSpace().WillBeReused();
            Console.WriteLine(sizeof(FileHeader));
		}
	}
}
