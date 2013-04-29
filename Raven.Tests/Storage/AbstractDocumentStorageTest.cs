//-----------------------------------------------------------------------
// <copyright file="AbstractDocumentStorageTest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database;
using Raven.Database.Extensions;
using Xunit;

namespace Raven.Tests.Storage
{
	public abstract class AbstractDocumentStorageTest : IDisposable
	{
		protected const string DataDir = "raven.db.test.esent";
		protected const string BackupDir = "raven.db.test.backup";

		protected AbstractDocumentStorageTest()
		{
			Delete();	 
		}

		public virtual void Dispose()
		{
			Delete();
		}

		private static void Delete()
		{
			IOExtensions.DeleteDirectory(DataDir);
			DeleteIfExists(BackupDir);
		}

		protected static void DeleteIfExists(string directoryName)
		{
			string directoryFullName = Path.IsPathRooted(directoryName)
										? directoryName
										: Path.Combine(AppDomain.CurrentDomain.BaseDirectory, directoryName);

			IOExtensions.DeleteDirectory(directoryFullName);
		}

		protected void WaitForBackup(DocumentDatabase db, bool checkError)
		{
			while (true)
			{
				var jsonDocument = db.Get(BackupStatus.RavenBackupStatusDocumentKey, null);
				if (jsonDocument == null)
					break;
				var backupStatus = jsonDocument.DataAsJson.JsonDeserialization<BackupStatus>();
				if (backupStatus.IsRunning == false)
				{
					if (checkError)
					{
						var firstOrDefault = backupStatus.Messages.FirstOrDefault(x => x.Severity == BackupStatus.BackupMessageSeverity.Error);
						if (firstOrDefault != null)
							Assert.False(true, firstOrDefault.Message);
					}

					return;
				}
				Thread.Sleep(50);
			}
		}
	}
}