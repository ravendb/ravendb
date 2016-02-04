// -----------------------------------------------------------------------
//  <copyright file="CounterSmugglerStreamSource.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Connection;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Database.Smuggler.Counter;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Json;
using Raven.Abstractions.Util;
using Raven.Database.Counters;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Smuggler.Common;

namespace Raven.Smuggler.Counter.Streams
{
    public class CounterSmugglerStreamSource : SmugglerStreamSourceBase, ICounterSmugglerSource
    {
        private readonly Stream _stream;

        private CountingStream _sizeStream;

        private StreamReader _streamReader;

        private JsonTextReader _reader;

        private CounterSmugglerOptions _options;

        private readonly bool _leaveOpen;

        public CounterSmugglerStreamSource(Stream stream, bool leaveOpen = true)
        {
            _stream = stream;
            _leaveOpen = leaveOpen;
        }

        public void Dispose()
        {
            _streamReader?.Dispose();

            _sizeStream?.Dispose();

            if (_leaveOpen == false)
                _stream?.Dispose();
        }

        public void OnException(SmugglerException exception)
        {
        }

        public Task InitializeAsync(CounterSmugglerOptions options, CancellationToken cancellationToken)
        {
            _options = options;

            _stream.Position = 0;
            _sizeStream = new CountingStream(new GZipStream(_stream, CompressionMode.Decompress));
            _streamReader = new StreamReader(_sizeStream);

            _reader = new RavenJsonTextReader(_streamReader);

            if (_reader.Read() == false)
                return new CompletedTask();

            if (_reader.TokenType != JsonToken.StartObject)
                throw new InvalidDataException("StartObject was expected");

            return new CompletedTask();
        }

        public bool SupportsMultipleSources => false;

        public IReadOnlyList<ICounterSmugglerSource> Sources => null;

        public bool SupportsRetries => false;

        public bool SupportsPaging => false;

        public Task AfterExecuteAsync(CounterSmugglerOperationState state)
        {
            return new CompletedTask();
        }

        public Task<CounterSmuggleType> GetNextSmuggleTypeAsync(CancellationToken cancellationToken)
        {
            while (_reader.Read() && _reader.TokenType != JsonToken.EndObject)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (_reader.TokenType != JsonToken.PropertyName)
                    throw new InvalidDataException("PropertyName was expected");

                var currentSection = _reader.Value.ToString();

                if (_reader.Read() == false)
                    return new CompletedTask<CounterSmuggleType>(CounterSmuggleType.None);

                if (_reader.TokenType != JsonToken.StartArray)
                    throw new InvalidDataException("StartArray was expected");

                switch (currentSection)
                {
                    case "CountersDeltas":
                        return new CompletedTask<CounterSmuggleType>(CounterSmuggleType.Delta);
                    case "CounterSnapshots":
                        return new CompletedTask<CounterSmuggleType>(CounterSmuggleType.Snapshots);
                    default:
                        throw new NotSupportedException("Not supported section: " + currentSection);
                }
            }

            return new CompletedTask<CounterSmuggleType>(CounterSmuggleType.None);
        }

        public Task SkipDeltasAsync(CancellationToken cancellationToken)
        {
            return SkipAsync(_reader, cancellationToken);
        }

        public Task SkipSnapshotsAsync(CancellationToken cancellationToken)
        {
            return SkipAsync(_reader, cancellationToken);
        }

        public Task<List<CounterSummary>> ReadSnapshotsAsync(int start, int pageSize, CancellationToken cancellationToken)
        {
            var results = new List<CounterSummary>();
            while (_reader.Read() && _reader.TokenType != JsonToken.EndArray)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var snapshot = RavenJToken.ReadFrom(_reader);

                var group = snapshot.Value<string>("Group");
                var name = snapshot.Value<string>("Name");
                var positive = snapshot.Value<long>("Positive");
                var negative = snapshot.Value<long>("Negative");

                var summary = new CounterSummary
                {
                    CounterName = name,
                    GroupName = group,
                    Decrements = negative,
                    Increments = positive
                };

                results.Add(summary);
            }

            return new CompletedTask<List<CounterSummary>>(results);
        }

        public Task<List<CounterState>> ReadDeltasAsync(int start, int pageSize, CancellationToken cancellationToken)
        {
            var results = new List<CounterState>();
            while (_reader.Read() && _reader.TokenType != JsonToken.EndArray)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var snapshot = RavenJToken.ReadFrom(_reader);

                var name = snapshot.Value<string>("CounterName");
                var group = snapshot.Value<string>("GroupName");
                var sign = snapshot.Value<char>("Sign");
                var value = snapshot.Value<long>("Value");

                var delta = new CounterState
                {
                    CounterName = name,
                    GroupName = group,
                    Value = value,
                    Sign = sign
                };

                results.Add(delta);
            }

            return new CompletedTask<List<CounterState>>(results);
        }
    }
}