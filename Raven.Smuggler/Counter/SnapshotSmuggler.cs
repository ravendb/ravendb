using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Counters;
using Raven.Abstractions.Database.Smuggler.Counter;
using Raven.Abstractions.Exceptions;
using Raven.Smuggler.Database;

namespace Raven.Smuggler.Counter
{
    internal class SnapshotSmuggler : CounterSmugglerBase
    {
        public SnapshotSmuggler(CounterSmugglerOptions options, CounterSmugglerNotifications notifications, ICounterSmugglerSource source, ICounterSmugglerDestination destination)
            : base(options, notifications, source, destination)
        {
        }

        public override async Task SmuggleAsync(CounterSmugglerOperationState state, CancellationToken cancellationToken)
        {
            using (var actions = Destination.SnapshotActions())
            {
                if (Destination.ImportSnapshots == false)
                {
                    await Source.SkipSnapshotsAsync(cancellationToken).ConfigureAwait(false);
                    return;
                }

                var count = 0;
                var retries = Source.SupportsRetries ? DatabaseSmuggler.NumberOfRetries : 1;
                var pageSize = Source.SupportsPaging ? Options.BatchSize : int.MaxValue;
                do
                {
                    List<CounterSummary> summaries;
                    try
                    {
                        summaries = await Source.ReadSnapshotsAsync(count, pageSize, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        if (retries-- == 0 && Options.IgnoreErrorsAndContinue)
                        {
                            Notifications.ShowProgress("Failed getting snapshots too much times, stopping the snapshot export entirely. Message: {0}", e.Message);
                            return;
                        }

                        if (Options.IgnoreErrorsAndContinue == false)
                            throw new SmugglerException(e.Message, e);

                        Notifications.ShowProgress("Failed fetching snapshots. {0} retries remaining. Message: {1}", retries, e.Message);
                        continue;
                    }

                    if (summaries.Count == 0)
                    {
                        Notifications.ShowProgress("Done with reading snapshots, total: {0}", count);
                        break;
                    }

                    count += summaries.Count;
                    Notifications.ShowProgress("Reading batch of {0,3} snapshots, read so far: {1,10:#,#;;0}", summaries.Count, count);

                    foreach (var summary in summaries)
                    {
                        try
                        {
                            await actions.WriteSnapshotAsync(summary, cancellationToken).ConfigureAwait(false);
                        }
                        catch (Exception e)
                        {
                            if (Options.IgnoreErrorsAndContinue == false)
                                throw new SmugglerException(e.Message, e);

                            Notifications.ShowProgress("Failed to export snapshot {0}. Message: {1}", summary, e.Message);
                        }
                    }
                } while (Source.SupportsPaging || Source.SupportsRetries);
            }
        }
    }
}