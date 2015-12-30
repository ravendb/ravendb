// -----------------------------------------------------------------------
//  <copyright file="FilesHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Net;

using Raven.Abstractions.Data;
using Raven.Client.FileSystem;

namespace Raven.Smuggler.FileSystem.Remote
{
    internal static class FileStoreHelper
    {
        internal static FilesStore CreateStore(FilesConnectionStringOptions options)
        {
            var credentials = options.Credentials as NetworkCredential;
            if (credentials == null)
            {
                credentials = CredentialCache.DefaultNetworkCredentials;
            }
            else if ((string.IsNullOrWhiteSpace(credentials.UserName) || string.IsNullOrWhiteSpace(credentials.Password)))
            {
                credentials = CredentialCache.DefaultNetworkCredentials;
            }

            var store = new FilesStore
            {
                Credentials = credentials,
                ApiKey = options.ApiKey,
                Url = options.Url,
                DefaultFileSystem = options.DefaultFileSystem,
            };

            store.Initialize(false);

            return store;
        }
    }
}