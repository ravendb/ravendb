using System;
using System.Runtime.InteropServices;

namespace Nevar.Tryouts
{
	class Program
	{

		[StructLayout(LayoutKind.Sequential, Pack = 0)]
		public struct PageHeader2
		{
			public int Num;
			public PageType Flags;

			public ushort Upper;
			public ushort Lower;


		}
		static unsafe void Main(string[] args)
		{
			Console.WriteLine(sizeof(PageHeader3));
			Console.WriteLine(Marshal.SizeOf(typeof(PageHeader3)));
		}
	}
}
