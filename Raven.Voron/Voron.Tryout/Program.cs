using System;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using System.Text;
using System.Xml;
using Voron.Debugging;
using Voron.Impl;
using Voron.Tests.Backups;
using Voron.Tests.Bugs;
using Voron.Tests.Storage;
using Snapshots = Voron.Tests.Bugs.Snapshots;

namespace Voron.Tryout
{
	internal unsafe class Program
	{
		[StructLayout(LayoutKind.Explicit)]
		private struct NumberWithHighAndLowParts
		{
			[FieldOffset(0)]
			public long Number;

			[FieldOffset(0)]
			public uint Low;

			[FieldOffset(4)]
			public uint High;
		}
		private static void Main()
		{
			for (int i = 0; i < 10; i++)
			{
			    Console.WriteLine(i);
				using (var x = new Incremental())
				{
					x.CanDoMultipleIncrementalBackupsAndRestoreOneByOne();
				}
			}
		}
	}
}