using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Rachis.Commands;
using Rachis.Interfaces;
using Rachis.Messages;

using Raven.Imports.Newtonsoft.Json;

namespace Rachis.Tests
{
    public class DictionaryStateMachine : IRaftStateMachine
    {
        private readonly JsonSerializer _serializer = new JsonSerializer();

        public long LastAppliedIndex
        {
            get { return Thread.VolatileRead(ref _lastAppliedIndex); }
            private set { Thread.VolatileWrite(ref _lastAppliedIndex, value); }
        }

        private class SnapshotWriter : ISnapshotWriter
        {
            private readonly Dictionary<string, int> _snapshot;
            private readonly DictionaryStateMachine _parent;

            public SnapshotWriter(DictionaryStateMachine parent, Dictionary<string, int> snapshot)
            {
                _parent = parent;
                _snapshot = snapshot;
            }

            public long Index { get; set; }
            public long Term { get; set; }
            public void WriteSnapshot(Stream stream)
            {
                var streamWriter = new StreamWriter(stream);
                _parent._serializer.Serialize(streamWriter, _snapshot);
                streamWriter.Flush();
            }
        }

        public ConcurrentDictionary<string, int> Data = new ConcurrentDictionary<string, int>();
        private SnapshotWriter _snapshot;
        private long _lastAppliedIndex;

        public void Apply(LogEntry entry, Command cmd)
        {
            if (LastAppliedIndex >= entry.Index)
                throw new InvalidOperationException("Already applied " + entry.Index);
            
            LastAppliedIndex = entry.Index;
            
            var dicCommand = cmd as DictionaryCommand;
            
            if (dicCommand != null) 
                dicCommand.Apply(Data);
        }

        public bool SupportSnapshots { get { return true; }}

        public void CreateSnapshot(long index, long term, ManualResetEventSlim allowFurtherModifications)
        {
            _snapshot = new SnapshotWriter(this, new Dictionary<string, int>(Data))
            {
                Term = term,
                Index = index
            };
            allowFurtherModifications.Set();
        }

        public ISnapshotWriter GetSnapshotWriter()
        {
            return _snapshot;
        }

        public void ApplySnapshot(long term, long index, Stream stream)
        {
            if(stream.CanSeek)
                stream.Position = 0;
            
            using (var streamReader = new StreamReader(stream))
                Data = new ConcurrentDictionary<string, int>(_serializer.Deserialize<Dictionary<string, int>>(new JsonTextReader(streamReader)));

            _snapshot = new SnapshotWriter(this, new Dictionary<string, int>(Data))
            {
                Term = term,
                Index = index
            };
        }

        public void Danger__SetLastApplied(long postion)
        {
            LastAppliedIndex = postion;
        }

        public void Dispose()
        {
            //nothing to do
        }
    }
}
