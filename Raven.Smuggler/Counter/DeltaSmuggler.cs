using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Database.Smuggler.Counter;
using Raven.Abstractions.Exceptions;
using Raven.Database.Counters;
using Raven.Smuggler.Database;

namespace Raven.Smuggler.Counter
{
    internal class DeltaSmuggler : CounterSmugglerBase
    {
        public DeltaSmuggler(CounterSmugglerOptions options, CounterSmugglerNotifications notifications, ICounterSmugglerSource source, ICounterSmugglerDestination destination)
            : base(options, notifications, source, destination)
        {
        }

        public override async Task SmuggleAsync(CounterSmugglerOperationState state, CancellationToken cancellationToken)
        {
            using (var actions = Destination.DeltaActions())
            {
                if (Destination.ImportDeltas == false)
                {
                    await Source.SkipDeltasAsync(cancellationToken).ConfigureAwait(false);
                    return;
                }

                var count = 0;
                var retries = Source.SupportsRetries ? DatabaseSmuggler.NumberOfRetries : 1;
                var pageSize = Source.SupportsPaging ? Options.BatchSize : int.MaxValue;
                do
                {
                    List<CounterState> deltas;
                    try
                    {
                        deltas = await Source.ReadDeltasAsync(count, pageSize, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        if (retries-- == 0 && Options.IgnoreErrorsAndContinue)
                        {
                            Notifications.ShowProgress("Failed getting deltas too much times, stopping the deltas export entirely. Message: {0}", e.Message);
                            return;
                        }

                        if (Options.IgnoreErrorsAndContinue == false)
                            throw new SmugglerException(e.Message, e);

                        Notifications.ShowProgress("Failed fetching deltas. {0} retries remaining. Message: {1}", retries, e.Message);
                        continue;
                    }

                    if (deltas.Count == 0)
                    {
                        Notifications.ShowProgress("Done with reading deltas, total: {0}", count);
                        break;
                    }

                    count += deltas.Count;
                    Notifications.ShowProgress("Reading batch of {0,3} deltas, read so far: {1,10:#,#;;0}", deltas.Count, count);

                    foreach (var delta in deltas)
                    {
                        try
                        {
                            await actions.WriteDeltaAsync(delta, cancellationToken).ConfigureAwait(false);
                        }
                        catch (Exception e)
                        {
                            if (Options.IgnoreErrorsAndContinue == false)
                                throw new SmugglerException(e.Message, e);

                            Notifications.ShowProgress("Failed to export delta {0}. Message: {1}", delta, e.Message);
                        }
                    }
                } while (Source.SupportsPaging || Source.SupportsRetries);
            }
        }
    }
}