// -----------------------------------------------------------------------
//  <copyright file="CounterSmugglerStreamDestination.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Database.Smuggler.Counter;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Smuggler.Counter.Streams
{
    public class CounterSmugglerStreamDestination : ICounterSmugglerDestination
    {
        protected Stream _stream;

        private readonly CounterSmugglerStreamDestinationOptions _options;

        private readonly bool _leaveOpen;

        private GZipStream _gZipStream;

        private StreamWriter _streamWriter;

        private JsonTextWriter _writer;

        public CounterSmugglerStreamDestination(Stream stream, CounterSmugglerStreamDestinationOptions options = null, bool leaveOpen = true)
        {
            _stream = stream;
            _options = options ?? new CounterSmugglerStreamDestinationOptions();
            _leaveOpen = leaveOpen;
        }

        public void OnException(SmugglerException exception)
        {
        }

        public Task InitializeAsync(CounterSmugglerOptions options, CancellationToken cancellationToken)
        {
            _gZipStream = new GZipStream(_stream, CompressionMode.Compress, leaveOpen: true);
            _streamWriter = new StreamWriter(_gZipStream);
            _writer = new JsonTextWriter(_streamWriter)
            {
                Formatting = Formatting.Indented
            };

            _writer.WriteStartObject();
            return new CompletedTask();
        }

        public Task AfterExecuteAsync(CounterSmugglerOperationState state)
        {
            return new CompletedTask();
        }

        public ICounterSmugglerDeltaActions DeltaActions()
        {
            return new CounterSmugglerStreamDeltaActions(_writer);
        }

        public ICounterSmugglerSnapshotActions SnapshotActions()
        {
            return new CounterSmugglerStreamSnapshotActions(_writer);
        }

        public bool ImportDeltas => _options.Incremental;

        public bool ImportSnapshots => _options.Incremental == false;

        public bool SupportsOperationState => false;

        public Task<CounterSmugglerOperationState> LoadOperationStateAsync(CounterSmugglerOptions options, CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        public void Dispose()
        {
            _writer?.WriteEndObject();
            _writer?.Flush();

            _streamWriter?.Flush();
            _streamWriter?.Dispose();

            _gZipStream?.Dispose();

            if (_leaveOpen == false)
            {
                _stream?.Flush();
                _stream?.Dispose();
            }
        }
    }
}