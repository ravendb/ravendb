// -----------------------------------------------------------------------
//  <copyright file="StandradBackup.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Xunit;

namespace Raven.Tests
{
	public class IncrementalBackupTest : RavenTest
	{
		private const string BackupDir = @".\BackupDatabase\";

		private class User
		{
			public string Name { get; set; }
		}

		public IncrementalBackupTest()
		{
			IOExtensions.DeleteDirectory(BackupDir);
		}

		[Fact]
		public void CreateIncrementalBackup()
		{
			var store = NewDocumentStore(requestedStorage: "esent");
			using (var session = store.OpenSession())
			{
				session.Store(new User { Name = "Fitzchak" });
				session.SaveChanges();
			}
			store.DocumentDatabase.StartBackup(BackupDir, true, new DatabaseDocument());
			WaitForBackup(store.DocumentDatabase, true);

			using (var session = store.OpenSession())
			{
				session.Store(new User { Name = "Oren" });
				session.SaveChanges();
			}
			store.DocumentDatabase.StartBackup(BackupDir, true, new DatabaseDocument());
			WaitForBackup(store.DocumentDatabase, true);
		}

		protected override void ModifyConfiguration(RavenConfiguration configuration)
		{
			configuration.Settings["Raven/Esent/CircularLog"] = "false";
			configuration.RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false;
		}

		public override void Dispose()
		{
			IOExtensions.DeleteDirectory(BackupDir);
		}
	}
}