using System;
using System.Threading;
using Voron.Tests.Backups;
using System.IO;
using Voron.Platform.Posix;
using Mono.Unix.Native;
using System.Runtime.InteropServices;
using System.Diagnostics;

namespace Voron.Tryout
{
	public unsafe class Program
	{
		public static void Main()
		{
			Console.WriteLine ("pid = "+ Process.GetCurrentProcess ().Id);
			Console.WriteLine ("press any key");
			Console.ReadKey ();
			//TestMemoryPager ();
			using (var test = new Full()) {
				test.CanBackupAndRestore ();
			}
		}

		static void TestMemoryPager ()
		{
			if (File.Exists ("test.p"))
				File.Delete ("test.p");
			var pager = new PosixMemoryMapPager ("test.p");
			pager.EnsureContinuous (null, 0, 150);
			var p = pager.AcquirePagePointer (0);
			for (int i = 0; i < 4096 * 150; i++) {
				*(p + i) = 1;
			}
			Console.WriteLine ("don");
		}
	}
}