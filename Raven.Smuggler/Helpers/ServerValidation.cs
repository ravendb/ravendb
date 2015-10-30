// -----------------------------------------------------------------------
//  <copyright file="ServerValidation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Connection;
using Raven.Abstractions.Exceptions;
using Raven.Client.Document;

namespace Raven.Smuggler.Helpers
{
    internal static class ServerValidation
    {
        public static async Task ValidateThatServerIsUpAndDatabaseExistsAsync(DocumentStore store, CancellationToken cancellationToken)
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
    }
}
