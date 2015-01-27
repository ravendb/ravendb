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
using System.Linq;
using Voron.Tests;
using Voron.Util;
using Xunit;
using System.Threading.Tasks;
using Voron.Tests.Bugs;

namespace Voron.Tryout
{
	public unsafe class Program
	{
		public static void Main()
		{
			var binaryNow = DateTime.Now.ToBinary();

			IntPtr ptr = Marshal.AllocHGlobal(Marshal.SizeOf(typeof(Test)));
			var pointer = (byte*)ptr.ToPointer();

			var sp = Stopwatch.StartNew();

			GCHandle gcHandle1 = GCHandle.Alloc(false, GCHandleType.Pinned);
			GCHandle gcHandle2 = GCHandle.Alloc(32, GCHandleType.Pinned);
			GCHandle gcHandle3 = GCHandle.Alloc(binaryNow, GCHandleType.Pinned);


			for (int i = 0; i < 1000*1000; i++)
			{
			//	var c = new Test()
			//	{
			//		bucket = 4,
			//		view = 32,
			//		timestampBinary = binaryNow
			//	};

				gcHandle1.Target = long.MaxValue;
				gcHandle2.Target = 32;
				gcHandle3.Target = binaryNow;

				MemoryUtils.Copy(pointer, (byte*)gcHandle1.AddrOfPinnedObject(), 4);
				MemoryUtils.Copy(pointer + 4, (byte*)gcHandle2.AddrOfPinnedObject(), 4);
				MemoryUtils.Copy(pointer + 8, (byte*)gcHandle3.AddrOfPinnedObject(), 8);



				//var p = ((Test*) pointer);
				//*p = c;
				//c = *p;
			}

			gcHandle1.Free();
			gcHandle2.Free();
			gcHandle3.Free();

			Console.WriteLine(sp.ElapsedMilliseconds);

			Marshal.FreeHGlobal(ptr);
			

		}

		[StructLayout(LayoutKind.Explicit, Pack = 1)]
 public struct Test
 {
     [FieldOffset(0)]
     public int view;
     [FieldOffset(4)]
     public int bucket;
     [FieldOffset(8)]
     public long timestampBinary;

 }

		static void RunAllTests ()
		{
			using (var fileWriter = new StreamWriter ("unit-tests.txt", append: false)) {
				var testAssembly = typeof(StorageTest).Assembly;
				var allTestClassTypes = testAssembly.GetTypes ().Where (t => t.IsSubclassOf (typeof(StorageTest))).ToList ();
				var allTestMethods = allTestClassTypes.SelectMany (t => t.GetMethods ().Where (mt => mt.GetCustomAttributes (true)
                                                                                               .OfType<FactAttribute> ().Any ()))
                    .OrderBy(x=>x.DeclaringType.Name + " " + x.Name)
                    .ToList ();
				var total = allTestMethods.Count;
				var failed = 0;
				Console.Clear ();
				fileWriter.WriteLine ("found " + total + " tests to run..");
				Console.WriteLine ("found " + total + " tests to run..");
				foreach (var classType in allTestClassTypes) {
					foreach (var testMethod in classType.GetMethods ()
					         							.Where (mt => 
					        								mt.GetCustomAttributes (true).OfType<FactAttribute> ().Any())
					         							.ToList())
					{
						Console.Write ("Running test: " + testMethod.Name + "...");
						bool isFailed = false;
						//create new test class instance for each unit test method - just like unit test runner does
						var testClassInstance = classType.GetConstructor (Type.EmptyTypes).Invoke (null);
						try{
						var sw = Stopwatch.StartNew ();
						fileWriter.Write ("Running test: " + testMethod.Name + "...");
						try {
								var testMethodTask = Task.Run (() => testMethod.Invoke (testClassInstance, null));
							if (!testMethodTask.Wait (10000)) {
								throw new TimeoutException ("The test " + testMethod + " has timed-out. Aborting execution");
							}
						} catch (Exception e) {
							fileWriter.WriteLine ("Test failed. \n Reason: " + e);
							failed++;
								isFailed = true;
						}
						fileWriter.WriteLine ("done. " + sw.ElapsedMilliseconds + "ms");
						fileWriter.WriteLine ("-----------------------------------------------------------");
						}
						finally{
							classType.GetMethod ("Dispose").Invoke (testClassInstance, null);
						}
						if (isFailed)
							Console.WriteLine ("failed");
						else
							Console.WriteLine ("succeeded");
					}
				}
				fileWriter.WriteLine ("------------------------------------------------");
				fileWriter.WriteLine ("------------------------------------------------");
				fileWriter.WriteLine ("Out of total " + total + ", failed: " + failed);
				fileWriter.Close ();
			}
			Console.WriteLine ("done");
		}

		static void TestEdgeCases ()
		{
			using (var test = new EdgeCases ()) {
				test.TransactionCommitShouldSetCurrentLogFileToNullIfItIsFull ();
			}
			Console.WriteLine ("done..");
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