using Mono.Unix.Native;
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
using Voron.Platform.Win32;
using Voron.Tests.Backups;
using Voron.Tests.Bugs;
using Voron.Tests.Storage;
using Voron.Trees;
using Snapshots = Voron.Tests.Bugs.Snapshots;

namespace Voron.Tryout
{
	public unsafe class Program
	{
		public static void Main()
		{
			var b = new MultiTransactions ();
				b.ShouldWork ();
			Console.WriteLine ("done");
		}
	}
}