// -----------------------------------------------------------------------
//  <copyright file="FilesEncryption.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Runtime.CompilerServices;

using Raven.Client.FileSystem;
using Raven.Database.Extensions;
using Raven.Tests.Common.Util;
using Raven.Tests.Helpers;

namespace Raven.Tests.FileSystem.Bundles.Encryption
{
    public class FileSystemEncryptionTest : RavenFilesTestBase
    {
        protected readonly string dataPath;

        public FileSystemEncryptionTest()
        {
            dataPath = NewDataPath("RavenFS_Encryption_Test", deleteOnDispose: false);
        }

        protected IAsyncFilesCommands NewAsyncClientForEncryptedFs(string requestedStorage, [CallerMemberName] string fileSystemName = null)
        {
            return NewAsyncClient(requestedStorage: requestedStorage, runInMemory: false, fileSystemName: fileSystemName, dataDirectory: dataPath, activeBundles: "Encryption", customConfig: configuration =>
            {
                configuration.Settings["Raven/Encryption/Key"] = "3w17MIVIBLSWZpzH0YarqRlR2+yHiv1Zq3TCWXLEMI8=";
            });
        }

        protected void Close()
        {
            base.Dispose();
        }

        protected void AssertPlainTextIsNotSavedInFileSystem(params string[] plaintext)
        {
            Close();

            EncryptionTestUtil.AssertPlainTextIsNotSavedInAnyFileInPath(plaintext, dataPath, s => true);
        }

        public override void Dispose()
        {
            Close();

            IOExtensions.DeleteDirectory(dataPath);
        }
    }
}
