using System;
using System.Threading;
using Voron.Tests.Backups;
using System.IO;
using Voron.Platform.Posix;
using Mono.Unix.Native;
using System.Runtime.InteropServices;
using System.Diagnostics;
using Voron.Tests.ScratchBuffer;
using Voron.Impl.Paging;
using Voron.Tests.Journal;

namespace Voron.Tryout
{
	public unsafe class Program
	{
		public static void Main()
		{
			Console.WriteLine ("pid = " + Process.GetCurrentProcess ().Id);
			Console.WriteLine ("press any key");
			Console.ReadKey ();
			using (var test = new EdgeCases ()) {
				test.TransactionCommitShouldSetCurrentLogFileToNullIfItIsFull ();
			}
			Console.WriteLine ("done..");

			//ScratchBufferGrowthTest ();

			//TestMemoryPager ();
			//TestPageFileBacked ();

		}

		static void TestPageFileBacked ()
		{
			if (File.Exists ("test.map"))
				File.Delete ("test.map");
			long initial = 4096;
			using (var pager = new PosixPageFileBackedMemoryMapPager ("test.map", initial)) {
				for (long size = initial; size < initial * 10000; size += 4096) {
					Console.WriteLine (size);
					pager.AllocateMorePages (null, size);
					pager.EnsureContinuous (null, 0, (int)size / AbstractPager.PageSize);
					var p = pager.AcquirePagePointer (0);
					for (int i = 0; i < size; i++) {
						*(p + i) = 1;
					}
				}
			}
		}

		static void ScratchBufferGrowthTest ()
		{
			using (var test = new MutipleScratchBuffersUsage ()) {
				test.CanAddContinuallyGrowingValue ();
			}
			Console.WriteLine ("done..");
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