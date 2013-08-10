using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq.Expressions;
using System.Runtime;
using Nevar.Debugging;
using Nevar.Impl;
using Nevar.Tests.Storage;
using Nevar.Tests.Trees;

namespace Nevar.Tryout
{
    internal class Program
    {
        private static void Main(string[] args)
        {
	        for (int i = 0; i < 30; i++)
	        {
		        Console.WriteLine("{0} -> {1}", i, PrevPowerOfTwo(i));
	        }
        }

		private static int PrevPowerOfTwo(int x)
		{
			if ((x & (x - 1)) == 0)
				return x;

			x--;
			x |= x >> 1;
			x |= x >> 2;
			x |= x >> 4;
			x |= x >> 8;
			x |= x >> 16;
			x++;

			return x >> 1;
		}
    }
}