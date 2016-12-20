//-----------------------------------------------------------------------
// <copyright file="Expiration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Tests.Common;

using Xunit;
using Raven.Database.Extensions;
using Xunit.Extensions;

namespace Raven.Tests.Bundles.Encryption
{
    public class EncryptionBackupAndRestore : RavenTest
    {
        [Theory]
        [PropertyData("Storages")]
        public async Task CanRestoreAnEncryptedDatabase(string storageEngineTypeName)
        {			
            IOExtensions.DeleteDirectory(@"~\Databases\Db1".ToFullPath());
            IOExtensions.DeleteDirectory(@"~\Databases\Db2".ToFullPath());

            using (var store = NewRemoteDocumentStore(requestedStorage: storageEngineTypeName, runInMemory: false))
            {
                var db1 = new DatabaseDocument
                {
                    Id = "Db1",
                    Settings = new Dictionary<string, string>
                    {
                        {"Raven/DataDir", @"~\Databases\Db1"},
                        {Constants.ActiveBundles, "Encryption"}
                    },
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
                var operation = await store.AsyncDatabaseCommands.ForDatabase("Db1").GlobalAdmin.StartBackupAsync(backupFolderDb1, db1, false, "Db1");
                operation.WaitForCompletion();

                await store.AsyncDatabaseCommands.GlobalAdmin.StartRestoreAsync(new DatabaseRestoreRequest
                {
                    BackupLocation = backupFolderDb1, 
                    DatabaseLocation = @"~\Databases\Db2", 
                    DatabaseName = "Db2"
                });
                WaitForRestore(store.DatabaseCommands.ForSystemDatabase());
                WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), "Raven/Databases/Db2");

                using (var session = store.OpenAsyncSession(Constants.SystemDatabase))
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
