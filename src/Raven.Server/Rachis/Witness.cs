using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Rachis.Remote;
using Raven.Server.Utils;

namespace Raven.Server.Rachis
{
    public class Witness:IDisposable
    {
        private static int _uniqueId;

        private readonly RachisConsensus _engine;
        private readonly long _term;
        private readonly RemoteConnection _connection;
        //private PoolOfThreads.LongRunningWork _followerLongRunningWork;

        private readonly string _debugName;
        private readonly RachisLogRecorder _debugRecorder;

        public Witness(RachisConsensus engine, long term, RemoteConnection remoteConnection)
        {
            _engine = engine;
            _connection = remoteConnection;
            _term = term;

            //for unique Id
            var uniqueId = Interlocked.Increment(ref _uniqueId);
            _debugName = $"Witness in term {_term} (id: {uniqueId})";
            _debugRecorder = _engine.InMemoryDebug.GetNewRecorder(_debugName);
            _debugRecorder.Start();
        }

        public override string ToString()
        {
            return $"Witness {_engine.Tag} of leader {_connection.Source} in term {_term}";
        }

        public void Dispose()
        {
            _connection.Dispose();
            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info($"{ToString()}: Disposing");
            }
            //if (_followerLongRunningWork != null && _followerLongRunningWork.ManagedThreadId != Thread.CurrentThread.ManagedThreadId)
            //    _followerLongRunningWork.Join(int.MaxValue);

            _engine.InMemoryDebug.RemoveRecorderOlderThan(DateTime.UtcNow.AddMinutes(-5));
        }
    }
}
