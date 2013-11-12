//-----------------------------------------------------------------------
// <copyright file="Expiration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Extensions;
using Xunit;

namespace Raven.Tests.Bundles.Encryption
{
	public class EncryptionBackupAndRestore : RavenTest
	{
		[Fact]
		public async Task CanRestoreAnEncryptedDatabase()
		{
			using (var store = NewRemoteDocumentStore(requestedStorage: "esent"))
			{
				var db1 = new DatabaseDocument
				{
					Id = "Db1",
					Settings = new Dictionary<string, string> {{"Raven/DataDir", @"~\Databases\Db1"}},
					SecuredSettings = new Dictionary<string, string>
					{
						{"Raven/Encryption/Key", "arHd5ENxwieUCAGkf4Rns8oPWx3f6npDgAowtIAPox0="},
						{"Raven/Encryption/Algorithm", "System.Security.Cryptography.DESCryptoServiceProvider, mscorlib"},
						{"Raven/Encryption/EncryptIndexes", "True"}
					},
				};
				await store.AsyncDatabaseCommands.GlobalAdmin.CreateDatabaseAsync(db1);

				using (var session = store.OpenAsyncSession("Db1"))
				{
					await session.StoreAsync(new User
					{
						Id = "users/1",
						Username = "fitzchak"
					});
					await session.SaveChangesAsync();
				}

				var backupFolderDb1 = NewDataPath("BackupFolderDb1");
				await store.AsyncDatabaseCommands.ForDatabase("Db1").Admin.StartBackupAsync(backupFolderDb1, db1);
				WaitForBackup(store.DatabaseCommands.ForDatabase("Db1"), true);

				await store.AsyncDatabaseCommands.Admin.StartRestoreAsync(backupFolderDb1, @"~\Databases\Db2", "Db2");
				WaitForRestore(store.DatabaseCommands);
				WaitForDocument(store.DatabaseCommands, "Raven/Databases/Db2");

				using (var session = store.OpenAsyncSession())
				{
					var db2Settings = await session.LoadAsync<DatabaseDocument>("Raven/Databases/Db2");
					Assert.NotEqual(db1.SecuredSettings["Raven/Encryption/Key"], db2Settings.SecuredSettings["Raven/Encryption/Key"]);
					Assert.NotEqual(db1.SecuredSettings["Raven/Encryption/Algorithm"], db2Settings.SecuredSettings["Raven/Encryption/Algorithm"]);
					Assert.NotEqual(db1.SecuredSettings["Raven/Encryption/EncryptIndexes"], db2Settings.SecuredSettings["Raven/Encryption/EncryptIndexes"]);
				}

				using (var session = store.OpenAsyncSession("Db2"))
				{
					var user = await session.LoadAsync<User>("users/1");
					Assert.NotNull(user);
					Assert.Equal("fitzchak", user.Username);
				}
			}
		}

		private class User
		{
			public string Id { get; set; }
			public string Username { get; set; }
		}
	}
}