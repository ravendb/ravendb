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
	public unsafe class Program
	{
		public static void Main()
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
		}
	}
}