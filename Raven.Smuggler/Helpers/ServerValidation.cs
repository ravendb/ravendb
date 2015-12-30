// -----------------------------------------------------------------------
//  <copyright file="ServerValidation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Util;
using Raven.Client.Document;
using Raven.Client.FileSystem;

namespace Raven.Smuggler.Helpers
{
    internal static class ServerValidation
    {
        internal static async Task ValidateThatServerIsUpAndDatabaseExistsAsync(DocumentStore store, CancellationToken cancellationToken)
        {
            try
            {
                await store
                    .AsyncDatabaseCommands
                    .GetStatisticsAsync(cancellationToken)
                    .ConfigureAwait(false); // check if database exist
            }
            catch (Exception e)
            {
                var responseException = e as ErrorResponseException;
                if (responseException != null && responseException.StatusCode == HttpStatusCode.ServiceUnavailable && (responseException.Message.StartsWith("Could not find a resource named") || responseException.Message.StartsWith("Could not find a database named")))
                    throw new SmugglerException(
                        string.Format(
                            "Smuggler does not support database creation (database '{0}' on server '{1}' must exist before running Smuggler).",
                            store.DefaultDatabase,
                            store.Url), e);


                if (e.InnerException != null)
                {
                    var webException = e.InnerException as WebException;
                    if (webException != null)
                    {
                        throw new SmugglerException(string.Format("Smuggler encountered a connection problem: '{0}'.", webException.Message), webException);
                    }
                }
                throw new SmugglerException(string.Format("Smuggler encountered a connection problem: '{0}'.", e.Message), e);
            }
        }

        internal static async Task ValidateThatServerIsUpAndFileSystemExists(FilesConnectionStringOptions server, FilesStore s)
        {
            var shouldDispose = false;

            try
            {
                var commands = !string.IsNullOrEmpty(server.DefaultFileSystem)
                                   ? s.AsyncFilesCommands.ForFileSystem(server.DefaultFileSystem)
                                   : s.AsyncFilesCommands;

                await commands.GetStatisticsAsync().ConfigureAwait(false); // check if file system exist
            }
            catch (Exception e)
            {
                shouldDispose = true;

                var responseException = e as ErrorResponseException;
                if (responseException != null && responseException.StatusCode == HttpStatusCode.ServiceUnavailable && responseException.Message.StartsWith("Could not find a resource named:"))
                    throw new SmugglerException(
                        string.Format(
                            "Smuggler does not support file system creation (file system '{0}' on server '{1}' must exist before running Smuggler).",
                            server.DefaultFileSystem,
                            s.Url), e);


                if (e.InnerException != null)
                {
                    var webException = e.InnerException as WebException;
                    if (webException != null)
                    {
                        throw new SmugglerException(string.Format("Smuggler encountered a connection problem: '{0}'.", webException.Message), webException);
                    }
                }
                throw new SmugglerException(string.Format("Smuggler encountered a connection problem: '{0}'.", e.Message), e);
            }
            finally
            {
                if (shouldDispose)
                    s.Dispose();
            }
        }

        internal static async Task DetectServerSupportedFeatures(ConnectionStringOptions connectionOptions)
        {
            using (var store = CreateDocumentStore(connectionOptions))
            {
                var serverVersion = (await store.AsyncDatabaseCommands.GlobalAdmin.GetBuildNumberAsync().ConfigureAwait(false)).ProductVersion;

                if (string.IsNullOrEmpty(serverVersion))
                    throw new SmugglerException("Server version is not available.");

                var smugglerVersion = FileVersionInfo.GetVersionInfo(AssemblyHelper.GetAssemblyLocationFor<Program>()).ProductVersion;
                var subServerVersion = serverVersion.Substring(0, 4);
                var subSmugglerVersion = smugglerVersion.Substring(0, 4);

                var intServerVersion = int.Parse(subServerVersion.Replace(".", string.Empty));
                if (intServerVersion < 40)
                    throw new SmugglerException(string.Format("This smuggler version requires a v4.0 or higher server. Smuggler version: {0}.", subSmugglerVersion));
            }
        }

        private static DocumentStore CreateDocumentStore(ConnectionStringOptions options)
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

            var store = new DocumentStore
            {
                Credentials = credentials,
                ApiKey = options.ApiKey,
                Url = options.Url,
            };

            store.Initialize();

            return store;
        }
    }
}
