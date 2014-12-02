using System;
using System.Threading;
using Voron.Tests.Backups;

namespace Voron.Tryout
{
	public unsafe class Program
	{
		public static void Main()
		{
			for (int i = 0; i < 1000; i++)
			{
				using (var test = new MinimalIncrementalBackupTests())
				{
					Console.WriteLine(i);
					test.Can_write_minimal_incremental_backup_and_restore_with_regular_incremental();
					Thread.Sleep(50);
				}
			}
		}
	}
}