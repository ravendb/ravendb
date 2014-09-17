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
using Voron.Tests.Backups;
using Voron.Tests.Bugs;
using Voron.Tests.Storage;
using Snapshots = Voron.Tests.Bugs.Snapshots;

namespace Voron.Tryout
{
	internal unsafe class Program
	{
		private static void Main()
		{
			using (var env = new StorageEnvironment (StorageEnvironmentOptions.ForPath ("VRN"))) 
			{
				var batch = new WriteBatch ();
				batch.Add ("ayende@ayende.com", "Oren Eini", "Names");

				env.Writer.Write (batch);
			}

			using (var env = new StorageEnvironment (StorageEnvironmentOptions.ForPath ("VRN"))) 
			{
				using (var snp = env.CreateSnapshot()) 
				{
					var reader = snp.Read ("Names", "ayende@ayende.com");
					if (reader == null) 
					{
						Console.WriteLine ("Couldn't find it");
					} 
					else 
					{
						Console.WriteLine (reader.Reader.ToStringValue());
					}
				}
			}
			Console.WriteLine ("Done");

//			var fd = Syscall.open ("test", Mono.Unix.Native.OpenFlags.O_RDWR | Mono.Unix.Native.OpenFlags.O_CREAT,
//			                       FilePermissions.ALLPERMS);
//			if(fd == -1)
//				Console.WriteLine ((Errno)Marshal.GetLastWin32Error ());
//			var a = Syscall.posix_fallocate (fd, 0, 16);
//			if(a != 0)
//				Console.WriteLine ((Errno)a + " a" );
//			var b = Syscall.mmap (IntPtr.Zero, 16, MmapProts.PROT_READ | MmapProts.PROT_WRITE,
//			                      MmapFlags.MAP_PRIVATE, fd, 0);
//			if(b .ToInt64() == -1)
//				Console.WriteLine ((Errno)Marshal.GetLastWin32Error ());
//			Console.WriteLine (b.ToString());
		}
	}
}