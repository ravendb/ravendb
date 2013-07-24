using System.IO;
using System.IO.MemoryMappedFiles;
using System.Text;
using Nevar.Debugging;
using Nevar.Impl;
using Nevar.Tests.Trees;

namespace Nevar.Tryout
{
	unsafe class Program
	{
		static void Main(string[] args)
		{
			new FreeSpace().WillBeReused();
		}
	}
}
