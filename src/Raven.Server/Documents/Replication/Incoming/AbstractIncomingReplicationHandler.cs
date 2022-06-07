using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server;
using Sparrow.Threading;
using Sparrow.Utils;

namespace Raven.Server.Documents.Replication.Incoming
{
    public abstract class AbstractIncomingReplicationHandler : IDisposable
    {
        protected PoolOfThreads.LongRunningWork _incomingWork;
        protected readonly DisposeOnce<SingleAttempt> _disposeOnce;
        protected Logger _log;
        protected readonly TcpClient _tcpClient;
        protected readonly Stream _stream;
        protected readonly TcpConnectionOptions _tcpConnectionOptions;
        private readonly ServerStore _server;
        protected readonly (IDisposable ReleaseBuffer, JsonOperationContext.MemoryBuffer Buffer) _copiedBuffer;
        protected readonly CancellationTokenSource _cts;
        protected StreamsTempFile _attachmentStreamsTempFile;

        public ServerStore Server => _server;
        public string DatabaseName { get; }

        public IncomingConnectionInfo ConnectionInfo { get; protected set; }

        public TcpConnectionHeaderMessage.SupportedFeatures SupportedFeatures { get; set; }

        protected string IncomingReplicationThreadName => $"Incoming replication {FromToString}";

        public virtual string FromToString => $"In database {Server.NodeTag}-{DatabaseName} @ {Server.GetNodeTcpServerUrl()} " +
                                              $"from {ConnectionInfo.SourceTag}-{ConnectionInfo.SourceDatabaseName} @ {ConnectionInfo.SourceUrl}";

        public bool IsDisposed => _disposeOnce.Disposed;

        protected AbstractIncomingReplicationHandler(TcpConnectionOptions tcpConnectionOptions, JsonOperationContext.MemoryBuffer buffer, ServerStore server, string databaseName, ReplicationLatestEtagRequest replicatedLastEtag, CancellationToken token)
        {
            _disposeOnce = new DisposeOnce<SingleAttempt>(DisposeInternal);
            _tcpConnectionOptions = tcpConnectionOptions;
            _copiedBuffer = buffer.Clone(_tcpConnectionOptions.ContextPool);
            _log = LoggingSource.Instance.GetLogger(databaseName, GetType().FullName);
            _tcpClient = tcpConnectionOptions.TcpClient;
            _stream = tcpConnectionOptions.Stream;
            _server = server;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(token);

            DatabaseName = databaseName;
            ConnectionInfo = IncomingConnectionInfo.FromGetLatestEtag(replicatedLastEtag);
            SupportedFeatures = TcpConnectionHeaderMessage.GetSupportedFeaturesFor(tcpConnectionOptions.Operation, tcpConnectionOptions.ProtocolVersion);
            ConnectionInfo.RemoteIp = ((IPEndPoint)_tcpClient.Client.RemoteEndPoint)?.Address.ToString();
        }

        protected abstract void ReceiveReplicationBatches();

        public void Start()
        {
            if (_incomingWork != null)
                return;

            lock (this)
            {
                if (_incomingWork != null)
                    return; // already set by someone else, they can start it

                _incomingWork = PoolOfThreads.GlobalRavenThreadPool.LongRunning(x => { DoIncomingReplication(); }, null, IncomingReplicationThreadName);
            }

            if (_log.IsInfoEnabled)
                _log.Info($"Incoming replication thread started ({FromToString})");
        }

        public void DoIncomingReplication()
        {
            try
            {
                ReceiveReplicationBatches();
            }
            catch (Exception e)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Error in accepting replication request ({FromToString})", e);
            }
        }

        protected ReplicationBatchItem ReadItemFromSource(Reader reader, JsonOperationContext context, ByteStringContext allocator, IncomingReplicationStatsScope stats)
        {
            stats.RecordInputAttempt();

            var item = ReplicationBatchItem.ReadTypeAndInstantiate(reader);
            item.ReadChangeVectorAndMarker();
            item.Read(context, allocator, stats);

            return item;
        }


        public void Dispose()
        {
            _disposeOnce.Dispose();
        }

        protected virtual void DisposeInternal()
        {
            var releaser = _copiedBuffer.ReleaseBuffer;
            try
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Disposing IncomingReplicationHandler ({FromToString})");
                _cts.Cancel();

                try
                {
                    _stream.Dispose();
                }
                catch (Exception)
                {
                }
                try
                {
                    _tcpClient.Dispose();
                }
                catch (Exception)
                {
                }

                try
                {
                    _tcpConnectionOptions.Dispose();
                }
                catch
                {
                    // do nothing
                }

                if (_incomingWork != PoolOfThreads.LongRunningWork.Current)
                {
                    try
                    {
                        _incomingWork?.Join(int.MaxValue);
                    }
                    catch (ThreadStateException)
                    {
                        // expected if the thread hasn't been started yet
                    }
                }
                _incomingWork = null;

                _cts.Dispose();

                _attachmentStreamsTempFile.Dispose();
            }
            finally
            {
                try
                {
                    releaser?.Dispose();
                }
                catch (Exception)
                {
                    // can't do anything about it...
                }
            }
        }

        public unsafe class IncomingReplicationAllocator : IDisposable
        {
            private readonly long _maxSizeForContextUseInBytes;
            private readonly long _minSizeToAllocateNonContextUseInBytes;
            public long TotalDocumentsSizeInBytes { get; private set; }

            private List<Allocation> _nativeAllocationList;
            private Allocation _currentAllocation;

            protected ByteStringContext _allocator;

            public IncomingReplicationAllocator(ByteStringContext allocator, Size? maxSizeToSend)
            {
                _allocator = allocator;
                var maxSizeForContextUse = maxSizeToSend * 2 ?? new Size(128, SizeUnit.Megabytes);

                _maxSizeForContextUseInBytes = maxSizeForContextUse.GetValue(SizeUnit.Bytes);
                var minSizeToNonContextAllocationInMb = PlatformDetails.Is32Bits ? 4 : 16;
                _minSizeToAllocateNonContextUseInBytes = new Size(minSizeToNonContextAllocationInMb, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes);
            }

            public byte* AllocateMemory(int size)
            {
                TotalDocumentsSizeInBytes += size;
                if (TotalDocumentsSizeInBytes <= _maxSizeForContextUseInBytes)
                {
                    _allocator.Allocate(size, out var output);
                    return output.Ptr;
                }

                if (_currentAllocation == null || _currentAllocation.Free < size)
                {
                    // first allocation or we don't have enough space on the currently allocated chunk

                    // there can be a document that is larger than the minimum
                    var sizeToAllocate = Math.Max(size, _minSizeToAllocateNonContextUseInBytes);

                    var allocation = new Allocation(sizeToAllocate);
                    if (_nativeAllocationList == null)
                        _nativeAllocationList = new List<Allocation>();

                    _nativeAllocationList.Add(allocation);
                    _currentAllocation = allocation;
                }

                return _currentAllocation.GetMemory(size);
            }

            public void Dispose()
            {
                if (_nativeAllocationList == null)
                    return;

                foreach (var allocation in _nativeAllocationList)
                {
                    allocation.Dispose();
                }
            }

            private class Allocation : IDisposable
            {
                private readonly byte* _ptr;
                private readonly long _allocationSize;
                private readonly NativeMemory.ThreadStats _threadStats;
                private long _used;
                public long Free => _allocationSize - _used;

                public Allocation(long allocationSize)
                {
                    _ptr = NativeMemory.AllocateMemory(allocationSize, out var threadStats);
                    _allocationSize = allocationSize;
                    _threadStats = threadStats;
                }

                public byte* GetMemory(long size)
                {
                    ThrowOnPointerOutOfRange(size);

                    var mem = _ptr + _used;
                    _used += size;
                    return mem;
                }

                [Conditional("DEBUG")]
                private void ThrowOnPointerOutOfRange(long size)
                {
                    if (_used + size > _allocationSize)
                        throw new InvalidOperationException(
                            $"Not enough space to allocate the requested size: {new Size(size, SizeUnit.Bytes)}, " +
                            $"used: {new Size(_used, SizeUnit.Bytes)}, " +
                            $"total allocation size: {new Size(_allocationSize, SizeUnit.Bytes)}");
                }

                public void Dispose()
                {
                    NativeMemory.Free(_ptr, _allocationSize, _threadStats);
                }
            }
        }
    }
}
