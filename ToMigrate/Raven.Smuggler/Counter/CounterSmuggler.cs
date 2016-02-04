// -----------------------------------------------------------------------
//  <copyright file="CounterSmuggler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Database.Smuggler.Counter;
using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Util;

namespace Raven.Smuggler.Counter
{
    public class CounterSmuggler
    {
        private readonly CounterSmugglerOptions _options;

        private readonly ICounterSmugglerSource _source;

        private readonly ICounterSmugglerDestination _destination;

        private readonly CounterSmugglerNotifications _notifications;

        public CounterSmuggler(CounterSmugglerOptions options, ICounterSmugglerSource source, ICounterSmugglerDestination destination)
        {
            _options = options;
            _source = source;
            _destination = destination;
            _notifications = new CounterSmugglerNotifications();
        }

        public CounterSmugglerNotifications Notifications
        {
            get
            {
                return _notifications;
            }
        }

        public CounterSmugglerOperationState Execute()
        {
            return AsyncHelpers.RunSync(() => ExecuteAsync(CancellationToken.None));
        }

        public async Task<CounterSmugglerOperationState> ExecuteAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            using (_source)
            using (_destination)
            {
                try
                {
                    await _source
                        .InitializeAsync(_options, cancellationToken)
                        .ConfigureAwait(false);

                    await _destination
                        .InitializeAsync(_options, cancellationToken)
                        .ConfigureAwait(false);

                    var state = await GetOperationStateAsync(_options, _source, _destination, cancellationToken).ConfigureAwait(false);

                    var sources = _source.SupportsMultipleSources
                        ? _source.Sources
                        : new List<ICounterSmugglerSource> { _source };

                    foreach (var source in sources)
                        await ProcessSourceAsync(source, state, cancellationToken).ConfigureAwait(false);

                    await _source.AfterExecuteAsync(state).ConfigureAwait(false);
                    await _destination.AfterExecuteAsync(state).ConfigureAwait(false);

                    return state;
                }
                catch (SmugglerException e)
                {
                    _source.OnException(e);
                    _destination.OnException(e);

                    throw;
                }
            }
        }

        private async Task ProcessSourceAsync(ICounterSmugglerSource source, CounterSmugglerOperationState state, CancellationToken cancellationToken)
        {
            while (true)
            {
                var type = await source
                    .GetNextSmuggleTypeAsync(cancellationToken)
                    .ConfigureAwait(false);

                switch (type)
                {
                    case CounterSmuggleType.None:
                        return;
                    case CounterSmuggleType.Delta:
                        await new DeltaSmuggler(_options, _notifications, source, _destination)
                            .SmuggleAsync(state, cancellationToken)
                            .ConfigureAwait(false);
                        continue;
                    case CounterSmuggleType.Snapshots:
                        await new SnapshotSmuggler(_options, _notifications, source, _destination)
                            .SmuggleAsync(state, cancellationToken)
                            .ConfigureAwait(false);
                        continue;
                    default:
                        throw new NotSupportedException(type.ToString());
                }
            }
        }

        private static async Task<CounterSmugglerOperationState> GetOperationStateAsync(CounterSmugglerOptions options, ICounterSmugglerSource source, ICounterSmugglerDestination destination, CancellationToken cancellationToken)
        {
            CounterSmugglerOperationState state = null;

            if (destination.SupportsOperationState)
            {
                state = await destination
                    .LoadOperationStateAsync(options, cancellationToken)
                    .ConfigureAwait(false);
            }

            if (state == null)
            {
                state = new CounterSmugglerOperationState
                {
                    LastEtag = options.StartEtag
                };
            }

            Debug.Assert(state.LastEtag != null);

            return state;
        }
    }
}