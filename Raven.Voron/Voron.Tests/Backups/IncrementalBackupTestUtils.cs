// -----------------------------------------------------------------------
//  <copyright file="IncrementalBackupTestUtils.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;

namespace Voron.Tests.Backups
{
	public class IncrementalBackupTestUtils
	{
		public static readonly Func<int, string> IncrementalBackupFile = n => string.Format("voron-test.{0}-incremental-backup.zip", n);
		public const string RestoredStoragePath = "incremental-backup-test.data";

		public static void Clean()
		{
			foreach (var incBackupFile in Directory.EnumerateFiles(".", "*incremental-backup.zip"))
			{
				File.Delete(incBackupFile);
			}

			if (Directory.Exists(RestoredStoragePath))
				Directory.Delete(RestoredStoragePath, true);
		} 
	}
}