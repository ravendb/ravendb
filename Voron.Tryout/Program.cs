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
//			MemoryMappedFile originalFile = null, additionToFile = null;
//			byte* baseAddr = null, tmpBaseAddr = null, unifiedBaseADdr = null;
//			byte* allocatedAddr = null;
//			NativeMethods.SYSTEM_INFO systemInfo;
//			NativeMethods.GetSystemInfo(out systemInfo);
//			
//			try
//			{
////				allocatedAddr = NativeMethods.VirtualAlloc(null, new UIntPtr(systemInfo.allocationGranularity * 2), 
////					NativeMethods.AllocationType.RESERVE, NativeMethods.MemoryProtection.EXECUTE_READWRITE);
//				originalFile = MemoryMappedFile.CreateNew("foo", systemInfo.allocationGranularity,MemoryMappedFileAccess.ReadWrite,MemoryMappedFileOptions.DelayAllocatePages,null,HandleInheritability.Inheritable);
//
//				baseAddr = MemoryMapNativeMethods.MapViewOfFileEx(originalFile.SafeMemoryMappedFileHandle.DangerousGetHandle(),
//					MemoryMapNativeMethods.NativeFileMapAccessType.AllAccess, 0, 0, new UIntPtr(systemInfo.allocationGranularity), null);
//				if (baseAddr == (byte*)0)
//					throw new Win32Exception(Marshal.GetLastWin32Error());
//
//				//additionToFile = MemoryMappedFile.CreateNew("foo_bar", systemInfo.allocationGranularity, MemoryMappedFileAccess.ReadWrite, MemoryMappedFileOptions.DelayAllocatePages, null, HandleInheritability.Inheritable);
//				var size = new NumberWithHighAndLowParts { Number = systemInfo.allocationGranularity };
//				var tmpFileMappingHandle = MemoryMapNativeMethods.CreateFileMapping(MemoryMapNativeMethods.INVALID_HANDLE_VALUE, IntPtr.Zero,
//					MemoryMapNativeMethods.FileMapProtection.PageReadWrite, size.High, size.Low, "foo_bar");
//				var lpBaseAddress = baseAddr + systemInfo.allocationGranularity;
//				tmpBaseAddr =
//					MemoryMapNativeMethods.MapViewOfFileEx(tmpFileMappingHandle,
//						MemoryMapNativeMethods.NativeFileMapAccessType.AllAccess, 0, 0, new UIntPtr(systemInfo.allocationGranularity),
//						lpBaseAddress);
//
//				if (tmpBaseAddr == (byte*)0)
//					throw new Win32Exception(Marshal.GetLastWin32Error());
//
//				//unifiedBaseADdr =
//				//	MemoryMapNativeMethods.MapViewOfFileEx(originalFile.SafeMemoryMappedFileHandle.DangerousGetHandle(),
//				//		MemoryMapNativeMethods.NativeFileMapAccessType.AllAccess, 0, 0, new UIntPtr(systemInfo.allocationGranularity * 2), baseAddr);
//				
//
//			}
//			finally
//			{
////				NativeMethods.VirtualFree(allocatedAddr, new UIntPtr(systemInfo.allocationGranularity * 2),NativeMethods.FreeType.MEM_RELEASE);
//				MemoryMapNativeMethods.UnmapViewOfFile(baseAddr);
//				MemoryMapNativeMethods.UnmapViewOfFile(tmpBaseAddr);
//				MemoryMapNativeMethods.UnmapViewOfFile(unifiedBaseADdr);
//				
//				if (originalFile != null) originalFile.Dispose();
//				if (additionToFile != null) additionToFile.Dispose();
//			}
//			string[] names =
//			{
//				"Treasa Tiano", "Arnette Arnone", "Michelina Matthias", "Reggie Royston",
//				"Rebekah Remy", "Meredith Marten", "Fletcher Fulton", "Gia Gibbens",
//				"Leon Lansing", "Malik Mcneal", "Dale Denbow",
//				"Barrett Bulfer", "Hee Heins", "Mitzie Mccourt", "Angela Arena",
//				"Jackelyn Johns", "Terri Toy", "Dinah Dinwiddie", "Sook Swasey",
//				"Wai Walko", "Corrin Cales", "Luciano Lenk", "Verline Vandusen",
//				"Joellen Joynes", "Babette Ballas", "Ebony Esh", "Josphine Junkin", "Herminia Horrigan",
//				"Chelsie Chiles", "Marlys Matheson", "Ruthanne Reilly",
//				"Teressa Tomasello", "Shani Squire", "Michaele Montagna",
//				"Cuc Corter", "Derek Devries", "Carylon Cupples", "Margaretta Mannings",
//				"Barbar Brunk", "Eboni Emond", "Genie Grosse",
//				"Kristin Krebsbach", "Livia Lecroy", "Jeraldine Jetton", "Jeanmarie Jan",
//				"Carmelo Coll", "Shizue Sugg", "Irena Imai", "Tam Troxel", "Berenice Burkart"
//			};
//			//using (var tx = db.NewTransaction(TransactionFlags.ReadWrite))
//			//{
//			//	int index = 0;
//			//	foreach (var name in names)
//			//	{
//			//		tx.State.Root.Add(tx, "users/" + (index++), new MemoryStream(Encoding.UTF8.GetBytes(name)));
//			//	}
//
//			//	tx.Commit();
//			//}
//
//			for (int i = 0; i < 100; i++)
//			{
//				Console.Write("{0,3} ", i);
//				try
//				{
//					using(var s = new RecoveryMultipleJournals())
//					{
//						s.CorruptingOneTransactionWillKillAllFutureTransactions();
//					}
//					Console.WriteLine("Success");
//				}
//				catch (Exception e)
//				{
//					Console.WriteLine("Failed");
//				}
//			}
		}
	}
}