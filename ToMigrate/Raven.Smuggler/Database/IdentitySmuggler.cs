// -----------------------------------------------------------------------
//  <copyright file="IdentitySmuggler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Abstractions.Exceptions;

namespace Raven.Smuggler.Database
{
    internal class IdentitySmuggler : SmugglerBase
    {
        public IdentitySmuggler(DatabaseSmugglerOptions options, DatabaseSmugglerNotifications notifications, IDatabaseSmugglerSource source, IDatabaseSmugglerDestination destination)
            : base(options, notifications, source, destination)
        {
        }

        public override async Task SmuggleAsync(DatabaseSmugglerOperationState state, CancellationToken cancellationToken)
        {
            using (var actions = Destination.IdentityActions())
            {
                if (Options.OperateOnTypes.HasFlag(DatabaseItemType.Documents) == false)
                {
                    await Source.SkipIdentitiesAsync(cancellationToken).ConfigureAwait(false);
                    return;
                }

                int readCount = 0, filteredCount = 0, writeCount = 0;
                var retries = Source.SupportsRetries ? DatabaseSmuggler.NumberOfRetries : 1;
                do
                {
                    List<KeyValuePair<string, long>> identities;
                    try
                    {
                        identities = await Source.ReadIdentitiesAsync(cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        if (retries-- == 0 && Options.IgnoreErrorsAndContinue)
                        {
                            Notifications.ShowProgress("Failed getting identities too much times, stopping the identity export entirely. Message: {0}", e.Message);
                            return;
                        }

                        if (Options.IgnoreErrorsAndContinue == false)
                            throw new SmugglerException(e.Message, e);

                        Notifications.ShowProgress("Failed fetching identities. {0} retries remaining. Message: {1}", retries, e.Message);
                        continue;
                    }

                    readCount += identities.Count;

                    Notifications.ShowProgress("Exported {0} following identities: {1}", identities.Count, string.Join(", ", identities.Select(x => x.Key)));

                    var filteredIdentities = identities.Where(x => FilterIdentity(x.Key, Options.OperateOnTypes)).ToList();

                    filteredCount += filteredIdentities.Count;

                    Notifications.ShowProgress("After filtering {0} identities need to be exported: {1}", filteredIdentities.Count, string.Join(", ", filteredIdentities.Select(x => x.Key)));

                    foreach (var identity in filteredIdentities)
                    {
                        try
                        {
                            await actions.WriteIdentityAsync(identity.Key, identity.Value, cancellationToken).ConfigureAwait(false);
                            writeCount++;
                        }
                        catch (Exception e)
                        {
                            if (Options.IgnoreErrorsAndContinue == false)
                                throw new SmugglerException(e.Message, e);

                            Notifications.ShowProgress("Failed to export identity {0}. Message: {1}", identity, e.Message);
                        }
                    }

                    break;
                } while (Source.SupportsRetries);

                Notifications.ShowProgress("IDENTITY. Read: {0}. Filtered: {1}. Wrote: {2}", readCount, readCount - filteredCount, writeCount);
            }
        }

        private static bool FilterIdentity(string indentityName, DatabaseItemType operateOnTypes)
        {
            if ("Raven/Etag".Equals(indentityName, StringComparison.InvariantCultureIgnoreCase))
                return false;

            if ("IndexId".Equals(indentityName, StringComparison.InvariantCultureIgnoreCase))
                return false;

            if (Constants.RavenSubscriptionsPrefix.Equals(indentityName, StringComparison.OrdinalIgnoreCase))
                return false;

            if (operateOnTypes.HasFlag(DatabaseItemType.Documents))
                return true;

            return false;
        }
    }
}
