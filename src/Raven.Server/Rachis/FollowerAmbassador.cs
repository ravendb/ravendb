using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.Rachis.Remote;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Server.Utils;
using Sparrow.Threading;
using Sparrow.Utils;
using Voron;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Data.Tables;
using Voron.Global;
using Memory = Sparrow.Memory;

namespace Raven.Server.Rachis
{
    public enum AmbassadorStatus
    {
        None,
        Started,
        Connected,
        FailedToConnect,
        Disconnected,
        Closed,
        Error,
    }

    public sealed class FollowerAmbassador : IDisposable
    {
        private readonly RachisConsensus _engine;
        private readonly Leader _leader;
        private ManualResetEvent _wakeLeader;
        private readonly string _tag;
        private readonly string _url;
        private string _statusMessage;

        public string StatusMessage
        {
            get => _statusMessage;
            set
            {
                if (_statusMessage == value)
                    return;

                _statusMessage = value;
                _engine.NotifyTopologyChange();
            }
        }
        public AmbassadorStatus Status;

        private long _followerMatchIndex;
        private long _followerLastCommitIndex;
        private long _lastReplyFromFollower;
        private long _lastSendToFollower;
        private string _lastSentMsg;
        private PoolOfThreads.LongRunningWork _followerAmbassadorLongRunningOperation;
        private RemoteConnection _connection;
        public RemoteConnection Connection => _connection;
        private readonly MultipleUseFlag _running = new MultipleUseFlag(true);
        private readonly long _term;

        private static int _uniqueId;

        public string Tag => _tag;

        public string ThreadStatus
        {
            get
            {
                if (_followerAmbassadorLongRunningOperation == null)
                    return "Did not start";
                if (_followerAmbassadorLongRunningOperation.Join(0))
                    return "Finished";
                return "Running";
            }
        }

        public long FollowerMatchIndex => Interlocked.Read(ref _followerMatchIndex);
        public long FollowerLastCommitIndex => Interlocked.Read(ref _followerLastCommitIndex);

        public DateTime LastReplyFromFollower => new DateTime(Interlocked.Read(ref _lastReplyFromFollower));
        public DateTime LastSendToFollower => new DateTime(Interlocked.Read(ref _lastSendToFollower));
        public string LastSendMsg => _lastSentMsg;
        public bool ForceElectionsNow { get; set; }
        public string Url => _url;

        public int FollowerCommandsVersion { get; set; }

        private readonly string _debugName;
        private readonly RachisLogRecorder _debugRecorder;

        private void UpdateLastSend(string msg)
        {
            Interlocked.Exchange(ref _lastSendToFollower, DateTime.UtcNow.Ticks);
            Interlocked.Exchange(ref _lastSentMsg, msg);
        }

        private void UpdateLastMatchFromFollower(long newVal)
        {
            Interlocked.Exchange(ref _lastReplyFromFollower, DateTime.UtcNow.Ticks);
            Interlocked.Exchange(ref _followerMatchIndex, newVal);
            _wakeLeader.Set();
        }

        private void UpdateLastMatchFromFollower(AppendEntriesResponse response)
        {
            Interlocked.Exchange(ref _followerLastCommitIndex, response.LastCommitIndex);
            UpdateLastMatchFromFollower(response.LastLogIndex);
        }

        private void UpdateFollowerTicks()
        {
            Interlocked.Exchange(ref _lastReplyFromFollower, DateTime.UtcNow.Ticks);
        }

        public FollowerAmbassador(RachisConsensus engine, Leader leader, ManualResetEvent wakeLeader, string tag, string url, RemoteConnection connection = null)
        {
            _engine = engine;
            _term = leader.Term;
            _leader = leader;
            _wakeLeader = wakeLeader;
            _tag = tag;
            _url = url;
            _connection = connection;
            Status = AmbassadorStatus.Started;
            StatusMessage = $"Started Follower Ambassador for {_engine.Tag} > {_tag} in term {_term}";
            var id = Interlocked.Increment(ref _uniqueId);
            _debugName = $"Follower Ambassador for {_tag} in term {_term} (id:{id})";
            _debugRecorder = _engine.InMemoryDebug.GetNewRecorder(_debugName);
        }

        public void UpdateLeaderWake(ManualResetEvent wakeLeader)
        {
            _wakeLeader = wakeLeader;
        }

        /// <summary>
        /// This method is expected to run for a long time (as long as we are the leader)
        /// it is responsible for talking to the remote follower and maintaining its state.
        /// This can never throw, and will run on its own thread.
        /// </summary>
        private unsafe void Run(object o)
        {
            _engine.ForTestingPurposes?.HoldOnLeaderElect?.BeforeNegotiatingWithFollower();

            try
            {
                ThreadHelper.TrySetThreadPriority(ThreadPriority.AboveNormal, ToString(), _engine.Log);

                var connectionBroken = false;
                var obtainConnectionFailure = false;
                var needNewConnection = _connection == null;
                while (_leader.Running && _running)
                {
                    _engine.ValidateLatestTerm(_term);
                    _debugRecorder.Start();
                    try
                    {
                        try
                        {
                            if (needNewConnection)
                            {
                                _debugRecorder.Record("Creating new connection to follower");
                                if (_engine.Log.IsInfoEnabled)
                                {
                                    _engine.Log.Info($"FollowerAmbassador for {_tag}: Creating new connection to {_tag}");
                                }

                                using (_engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                                {
                                    _engine.RemoveAndDispose(_leader, _connection);

                                    var connectTask = _engine.ConnectToPeer(_url, _tag, _engine.ClusterCertificate);

                                    if (WaitForConnection(connectTask) == false)
                                        return;

                                    var connection = connectTask.Result;
                                    var stream = connection.Stream;
                                    var disconnect = connection.Disconnect;
                                    var con = new RemoteConnection(_tag, _engine.Tag, _term, stream, connection.SupportedFeatures, disconnect);
                                    Interlocked.Exchange(ref _connection, con);

                                    ClusterTopology topology;
                                    using (context.OpenReadTransaction())
                                    {
                                        topology = _engine.GetTopology(context);
                                    }

                                    SendHello(context, topology);
                                }
                            }
                            else
                            {
                                // if we are here we have won the elections, let's notify the elector about it.
                                if (_engine.Log.IsInfoEnabled)
                                {
                                    _engine.Log.Info($"Follower ambassador reuses the connection for {_tag} and send a winning message to his elector.");
                                }

                                CandidateAmbassador.SendElectionResult(_engine, _connection, _term, ElectionResult.Won);
                            }
                        }
                        catch (Exception e)
                        {
                            if (e is not ParentStateChangedConcurrencyException)
                                NotifyOnException(ref obtainConnectionFailure, $"Failed to create a connection to node {_tag} at {_url}", e);

                            _leader.WaitForNewEntries().Wait(TimeSpan.FromMilliseconds(_engine.ElectionTimeout.TotalMilliseconds / 2));
                            continue; // we'll retry connecting
                        }
                        finally
                        {
                            needNewConnection = true;
                        }

                        obtainConnectionFailure = false;
                        // TODO: Dismiss notification

                        _debugRecorder.Record("Connection obtained");
                        Status = AmbassadorStatus.Connected;
                        StatusMessage = $"Connected with {_tag}";

                        try
                        {
                            _engine.AppendStateDisposable(_leader, _connection);
                        }
                        catch (ConcurrencyException)
                        {
                            // we are no longer the leader, but we'll not abort the thread here, we'll 
                            // go to the top of the while loop and exit from there if needed
                            continue;
                        }

                        _debugRecorder.Record("Start negotiation with follower");
                        var matchIndex = InitialNegotiationWithFollower();
                        _debugRecorder.Record($"Found highest common index: {matchIndex}");
                        UpdateLastMatchFromFollower(matchIndex);
                        SendSnapshot(_connection.Stream);

                        var entries = new List<BlittableJsonReaderObject>();
                        var readWatcher = Stopwatch.StartNew();
                        while (_leader.Running && _running)
                        {
                            var myLastCommittedIndex = 0L;
                            entries.Clear();
                            _engine.ValidateLatestTerm(_term);

                            if (_engine.ForTestingPurposes?.NodeTagsToDisconnect?.Contains(_tag) == true)
                            {
                                throw new InvalidOperationException($"Exception was thrown for disconnecting  node {_tag} - For testing purposes.");
                            }

                            using (_engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                            {
                                AppendEntries appendEntries;
                                using (context.OpenReadTransaction())
                                {
                                    var table = context.Transaction.InnerTransaction.OpenTable(RachisConsensus.LogsTable, RachisConsensus.EntriesSlice);

                                    var reveredNextIndex = Bits.SwapBytes(_followerMatchIndex + 1);
                                    using (Slice.External(context.Allocator, (byte*)&reveredNextIndex, sizeof(long), out Slice key))
                                    {
                                        long totalSize = 0;
                                        foreach (var value in table.SeekByPrimaryKey(key, 0))
                                        {
                                            var entry = BuildRachisEntryToSend(context, value);
                                            _engine.Validator.AssertEntryBeforeSendToFollower(entry, FollowerCommandsVersion, _tag);
                                            entries.Add(entry);
                                            totalSize += entry.Size;
                                            if (totalSize > Constants.Size.Megabyte)
                                                break;
                                        }

                                        appendEntries = new AppendEntries
                                        {
                                            ForceElections = ForceElectionsNow,
                                            EntriesCount = entries.Count,
                                            LeaderCommit = _engine.GetLastCommitIndex(context),
                                            Term = _term,
                                            TruncateLogBefore = _leader.LowestIndexInEntireCluster,
                                            PrevLogTerm = _engine.GetTermFor(context, _followerMatchIndex) ?? 0,
                                            PrevLogIndex = _followerMatchIndex,
                                            TimeAsLeader = _leader.LeaderShipDuration,
                                            MinCommandVersion = _engine.CommandsVersionManager.CurrentClusterMinimalVersion
                                        };

                                        myLastCommittedIndex = appendEntries.LeaderCommit;
                                    }
                                }

                                // out of the tx, we can do network calls
                                UpdateLastSend(
                                    entries.Count > 0
                                        ? "Append Entries"
                                        : "Heartbeat"
                                );

                                if (_engine.Log.IsDebugEnabled && entries.Count > 0)
                                {
                                    _engine.Log.Debug($"FollowerAmbassador for {_tag}: sending {entries.Count} entries to {_tag}"
#if DEBUG
                                                     + $" [{string.Join(" ,", entries.Select(x => x.ToString()))}]"
#endif
                                    );
                                }

                                _debugRecorder.Record(entries.Count > 0
                                    ? $"Sending {entries.Count} Entries"
                                    : "Sending Heartbeat");

                                _connection.Send(context, UpdateFollowerTicks, appendEntries, entries);
                                _debugRecorder.Record("Waiting for response");
                                AppendEntriesResponse aer = null;
                                while (true)
                                {
                                    readWatcher.Restart();
                                    try
                                    {
                                        aer = _connection.Read<AppendEntriesResponse>(context);
                                    }
                                    finally
                                    {
                                        if (readWatcher.Elapsed > _engine.ElectionTimeout / 2)
                                        {
                                            if (_engine.Log.IsInfoEnabled)
                                            {
                                                var msg = aer?.Success == true ? "successfully" : "failed";
                                                _engine.Log.Info(
                                                    $"{ToString()}: waited long time ({readWatcher.ElapsedMilliseconds}) to read a single response from stream ({msg}).");
                                            }
                                        }
                                    }

                                    if (aer.Pending == false)
                                        break;
                                    UpdateFollowerTicks();
                                }

                                _debugRecorder.Record("Response was received");
                                if (aer.Success == false)
                                {
                                    // shouldn't happen, the connection should be aborted if this is the case, but still
                                    var msg =
                                        "A negative Append Entries Response after the connection has been established shouldn't happen. Message: " +
                                        aer.Message;
                                    if (_engine.Log.IsInfoEnabled)
                                    {
                                        _engine.Log.Info($"{ToString()}: failure to append entries to {_tag} because: " + msg);
                                    }

                                    RachisInvalidOperationException.Throw(msg);
                                }

                                if (aer.CurrentTerm != _term)
                                {
                                    var msg = $"The current engine term has changed " +
                                              $"({aer.CurrentTerm:#,#;;0} -> {_term:#,#;;0}), " +
                                              $"this ambassador term is no longer valid";
                                    if (_engine.Log.IsInfoEnabled)
                                    {
                                        _engine.Log.Info($"{ToString()}: failure to append entries to {_tag} because: {msg}");
                                    }

                                    RachisConcurrencyException.Throw(msg);
                                }

                                UpdateLastMatchFromFollower(aer);
                            }

                            if (_running == false)
                                break;

                            connectionBroken = false;
                            // TODO: Dismiss notification

                            var task = _leader.WaitForNewEntries();
                            using (_engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                            using (context.OpenReadTransaction())
                            {
                                if (_engine.GetLastEntryIndex(context) != _followerMatchIndex)
                                    continue; // instead of waiting, we have new entries, start immediately

                                if (_engine.GetLastCommitIndex(context) != myLastCommittedIndex)
                                    continue; // there is a new committed command, continue to let the leader know immediately
                            }

                            // either we have new entries to send, or we waited for long enough 
                            // to send another heartbeat
                            task.Wait(TimeSpan.FromMilliseconds(_engine.ElectionTimeout.TotalMilliseconds / 3));
                            UpdateFollowerTicks(); // keep the leader in full confidence of his leadership 
                            _debugRecorder.Record("Cycle done");
                            _debugRecorder.Start();
                        }

                        Status = AmbassadorStatus.Disconnected;
                        StatusMessage = "Graceful shutdown";
                        _debugRecorder.Record(StatusMessage);
                        return;
                    }
                    catch (RachisConcurrencyException)
                    {
                        // our term is no longer valid
                        throw;
                    }
                    catch (RachisException)
                    {
                        // this is a rachis protocol violation exception, we must close this ambassador. 
                        throw;
                    }
                    catch (Exception e)
                    {
                        NotifyOnException(ref connectionBroken, $"The connection with node {_tag} was suddenly broken.", e);

                        if (e is TopologyMismatchException)
                        {
                            if (_leader.TryModifyTopology(_tag, _url, Leader.TopologyModification.Remove, out _))
                            {
                                StatusMessage = "No longer in the topology";
                                Status = AmbassadorStatus.Disconnected;
                                return;
                            }
                        }
                        // This is an unexpected exception which indicate the something is wrong with the connection.
                        // So we will retry to reconnect. 
                        _connection?.Dispose();
                        _leader.WaitForNewEntries().Wait(TimeSpan.FromMilliseconds(_engine.ElectionTimeout.TotalMilliseconds / 2));
                    }
                }

                Status = AmbassadorStatus.Disconnected;
                StatusMessage = "Graceful shutdown";
            }
            catch (RachisException e)
            {
                StatusMessage = $"Reached an erroneous state due to :{Environment.NewLine}{e.Message}";
                Status = AmbassadorStatus.Error;
            }
            catch (Exception e) when (RachisConsensus.IsExpectedException(e))
            {
                StatusMessage = "Closed";
                Status = AmbassadorStatus.Closed;
            }
            catch (Exception e)
            {
                StatusMessage = $"Failed to talk with {_tag}.{Environment.NewLine}" + e.Message;
                Status = AmbassadorStatus.FailedToConnect;

                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info("Failed to talk to remote follower: " + _tag, e);
                }
            }
            finally
            {
                _debugRecorder.Record(StatusMessage);
                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info($"{ToString()}: Node {_tag} is finished with the message '{StatusMessage}'.");
                }
                _connection?.Dispose();
            }
        }

        private bool WaitForConnection(Task<RachisConnection> connectTask)
        {
            try
            {
                while (connectTask.Wait(1000) == false)
                {
                    if (_leader.Running == false ||
                        _running == false)
                    {
                        connectTask.ContinueWith(RachisConsensus.DisconnectAction, TaskContinuationOptions.ExecuteSynchronously);
                        return false;
                    }

                    _engine.ValidateLatestTerm(_term);
                }
            }
            catch
            {
                connectTask.ContinueWith(RachisConsensus.DisconnectAction, TaskContinuationOptions.ExecuteSynchronously);
                throw;
            }

            return true;
        }


        private void NotifyOnException(ref bool hadConnectionFailure, string message, Exception e)
        {
            // It could be that due to election or leader change, the follower has forcefully closed the connection.
            // In any case we don't want to raise a notification due to a one-time connection failure.
            var isGracefulError = IsGracefulError(e);
            if (hadConnectionFailure || isGracefulError == false)
            {
                //We don't want to notify about cluster change every time we check the connection, only if the change is new
                if (Status != AmbassadorStatus.FailedToConnect)
                {
                    Status = AmbassadorStatus.FailedToConnect;
                    StatusMessage = $"{message}.{Environment.NewLine}" + e;

                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info(message, e);
                    }

                    _leader?.NotifyAboutException(_tag, $"Node {_tag} encountered an error", message, e);
                }
            }

            if (isGracefulError)
            {
                hadConnectionFailure = true;
            }
            _debugRecorder.Record(e.ToString());
        }

        private bool IsGracefulError(Exception e)
        {
            if (e is AggregateException)
                return IsGracefulError(e.InnerException);

            if (e is LockAlreadyDisposedException)
                return true;

            if (e is TaskCanceledException)
                return true;

            if (e is IOException)
                return true;

            return false;
        }

        private void SendSnapshot(Stream stream)
        {
            _debugRecorder.Record("Begin sending Snapshot...");

            using (_engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                var earliestIndexEntry = _engine.GetFirstEntryIndex(context);

                if (_followerMatchIndex >= earliestIndexEntry ||
                    _followerMatchIndex == earliestIndexEntry - 1) // In case the first entry is the next entry to send
                {
                    // we don't need a snapshot, so just send updated topology
                    UpdateLastSend("Send empty snapshot");
                    if (_engine.Log.IsDebugEnabled)
                    {
                        _engine.Log.Debug($"{ToString()}: sending empty snapshot to {_tag}");
                    }

                    _connection.Send(context, new InstallSnapshot
                    {
                        LastIncludedIndex = _followerMatchIndex,
                        LastIncludedTerm = _engine.GetTermForKnownExisting(context, _followerMatchIndex),
                        Topology = _engine.GetTopologyRaw(context)
                    });
                    using (var binaryWriter = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true))
                    {
                        binaryWriter.Write(-1);
                    }
                }
                else
                {
                    _engine.GetLastCommitIndex(context, out long index, out long term);
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info($"{ToString()}: sending snapshot to {_tag} with index={index} term={term}");
                    }

                    // we make sure that we routinely update LastReplyFromFollower here
                    // so we'll not leave the leader thinking we abandoned it
                    UpdateFollowerTicks();
                    UpdateLastSend("Send full snapshot");
                    _connection.Send(context, new InstallSnapshot
                    {
                        LastIncludedIndex = index,
                        LastIncludedTerm = term,
                        Topology = _engine.GetTopologyRaw(context)
                    });

                    WriteSnapshotToFile(context, new BufferedStream(stream));
                    UpdateFollowerTicks();
                }
                _debugRecorder.Record("Sending snapshot is completed, waiting for an ack from the follower");
                while (true)
                {
                    var aer = _connection.Read<InstallSnapshotResponse>(context);
                    if (aer.Done)
                    {
                        UpdateLastMatchFromFollower(aer.LastLogIndex);
                        break;
                    }
                    _debugRecorder.Record("Follower is alive, but didn't completed to commit the snapshot yet");
                    UpdateFollowerTicks();
                }
                _debugRecorder.Record("Sending and installing the snapshot is completed");

                if (_engine.Log.IsDebugEnabled)
                {
                    _engine.Log.Debug($"{ToString()}: done sending snapshot to {_tag}");
                }
            }
        }

        private unsafe void WriteSnapshotToFile(ClusterOperationContext context, Stream dest)
        {
            var dueTime = (int)(_engine.ElectionTimeout.TotalMilliseconds / 3);
            var timer = new Timer(_ => UpdateFollowerTicks(), null, dueTime, dueTime);
            long totalSizeInBytes = 0;
            long items = 0;
            var sp = Stopwatch.StartNew();

            try
            {
                var copier = new UnmanagedMemoryToStream(dest);

                using (var binaryWriter = new BinaryWriter(dest, Encoding.UTF8, leaveOpen: true))
                {
                    var txr = context.Transaction.InnerTransaction;
                    var llt = txr.LowLevelTransaction;
                    using (var rootIterator = llt.RootObjects.Iterate(false))
                    {
                        if (rootIterator.Seek(Slices.BeforeAllKeys) == false)
                            throw new InvalidOperationException("Root objects iterations must always have _something_!");
                        do
                        {
                            var rootObjectType = txr.GetRootObjectType(rootIterator.CurrentKey);
                            if (_engine.ShouldSnapshot(rootIterator.CurrentKey, rootObjectType) == false)
                                continue;
                            
                            _debugRecorder.Record($"Start sending: '{rootIterator.CurrentKey}'");

                            var currentTreeKey = rootIterator.CurrentKey;
                            binaryWriter.Write((int)rootObjectType);
                            binaryWriter.Write(currentTreeKey.Size);
                            copier.Copy(currentTreeKey.Content.Ptr, currentTreeKey.Size);
                            CalculateTotalSize(ref items, ref totalSizeInBytes, sizeof(long) + currentTreeKey.Size);

                            switch (rootObjectType)
                            {
                                case RootObjectType.VariableSizeTree:
                                    var tree = txr.ReadTree(currentTreeKey);

                                    ref readonly var header = ref tree.ReadHeader();
                                    binaryWriter.Write(header.NumberOfEntries);
                                    CalculateTotalSize(ref items, ref totalSizeInBytes, sizeof(long));

                                    var type = header.Flags;
                                    if (_connection.Features.Cluster.MultiTree)
                                    {
                                        binaryWriter.Write((int)type);
                                        CalculateTotalSize(ref items, ref totalSizeInBytes, sizeof(int));
                                    }

                                    using (var treeIterator = tree.Iterate(false))
                                    {
                                        if (treeIterator.Seek(Slices.BeforeAllKeys))
                                        {
                                            do
                                            {
                                                var currentTreeValueKey = treeIterator.CurrentKey;
                                                binaryWriter.Write(currentTreeValueKey.Size);
                                                copier.Copy(currentTreeValueKey.Content.Ptr, currentTreeValueKey.Size);
                                                CalculateTotalSize(ref items, ref totalSizeInBytes, sizeof(int) + currentTreeValueKey.Size);

                                                switch (type)
                                                {
                                                    case TreeFlags.None:
                                                        var reader = treeIterator.CreateReaderForCurrent();
                                                        binaryWriter.Write(reader.Length);
                                                        copier.Copy(reader.Base, reader.Length);

                                                        CalculateTotalSize(ref items, ref totalSizeInBytes, sizeof(int) + reader.Length);
                                                        break;

                                                    case TreeFlags.MultiValueTrees:

                                                        if (_connection.Features.Cluster.MultiTree == false)
                                                            throw new NotSupportedException(
                                                                $"The connection '{_connection}' doesn't support '{type}', please upgrade node '{_connection.Dest}'");

                                                        long count = tree.MultiCount(currentTreeValueKey);
                                                        binaryWriter.Write(count);
                                                        CalculateTotalSize(ref items, ref totalSizeInBytes, sizeof(long));

                                                        using (var multiIt = tree.MultiRead(currentTreeValueKey))
                                                        {
                                                            if (multiIt.Seek(Slices.BeforeAllKeys))
                                                            {
                                                                do
                                                                {
                                                                    var val = multiIt.CurrentKey;
                                                                    binaryWriter.Write(val.Size);
                                                                    copier.Copy(val.Content.Ptr, val.Size);
                                                                    CalculateTotalSize(ref items, ref totalSizeInBytes, val.Size + sizeof(int));
                                                                } while (multiIt.MoveNext());
                                                            }
                                                        }

                                                        break;
                                                    default:
                                                        throw new ArgumentOutOfRangeException($"Can't send snapshot of type {type}");
                                                }
                                            } while (treeIterator.MoveNext());
                                        }
                                    }
                                    break;
                                case RootObjectType.Table:
                                    var tableTree = txr.ReadTree(currentTreeKey, RootObjectType.Table);

                                    // Get the table schema
                                    var schemaSize = tableTree.GetDataSize(TableSchema.SchemasSlice);
                                    var schemaPtr = tableTree.DirectRead(TableSchema.SchemasSlice);
                                    var schema = TableSchema.ReadFrom(txr.Allocator, schemaPtr, schemaSize);

                                    // Load table into structure 
                                    var inputTable = txr.OpenTable(schema, currentTreeKey);
                                    binaryWriter.Write(inputTable.NumberOfEntries);
                                    CalculateTotalSize(ref items, ref totalSizeInBytes, sizeof(long));

                                    foreach (var holder in inputTable.SeekByPrimaryKey(Slices.BeforeAllKeys, 0))
                                    {
                                        binaryWriter.Write(holder.Reader.Size);
                                        copier.Copy(holder.Reader.Pointer, holder.Reader.Size);
                                        CalculateTotalSize(ref items, ref totalSizeInBytes, sizeof(int) + holder.Reader.Size);
                                    }
                                    break;
                                default:
                                    throw new ArgumentOutOfRangeException(nameof(rootObjectType), rootObjectType + " " + rootIterator.CurrentKey);
                            }

                        } while (rootIterator.MoveNext());
                    }

                    binaryWriter.Write((int)RootObjectType.None);
                    CalculateTotalSize(ref items, ref totalSizeInBytes, sizeof(int));
                }

                dest.Flush();
            }
            finally
            {
                var mre = new ManualResetEvent(false);
                timer.Dispose(mre);
                while (mre.WaitOne(dueTime) == false)
                {
                    UpdateFollowerTicks();
                }
            }

            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info($"Sending snapshot to {_tag}, " +
                                 $"total size: {new Size(totalSizeInBytes, SizeUnit.Bytes)}, " +
                                 $"took: {sp.ElapsedMilliseconds}ms");
            }
        }

        private void CalculateTotalSize(ref long items, ref long totalSizeInBytes, long sizeToIncrease)
        {
            totalSizeInBytes += sizeToIncrease;
            if (++items % 128 == 0)
            {
                _debugRecorder.Record($"Sent total of {new Size(totalSizeInBytes, SizeUnit.Bytes)}");
            }
        }
        private sealed unsafe class UnmanagedMemoryToStream
        {
            private readonly byte[] _buffer = new byte[1024];

            private readonly Stream _stream;

            public UnmanagedMemoryToStream(Stream stream)
            {
                _stream = stream;
            }

            public void Copy(byte* ptr, int size)
            {
                fixed (byte* pBuffer = _buffer)
                {
                    while (size > 0)
                    {
                        var written = Math.Min(size, _buffer.Length);
                        Memory.Copy(pBuffer, ptr, written);

                        _stream.Write(_buffer, 0, written);

                        ptr += written;
                        size -= written;
                    }
                }
            }
        }

        internal static unsafe BlittableJsonReaderObject BuildRachisEntryToSend<TTransaction>(TransactionOperationContext<TTransaction> context, Table.TableValueHolder value)
            where TTransaction : RavenTransaction
        {
            BlittableJsonReaderObject entry;
            using (var writer =
                new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(
                    context, BlittableJsonDocumentBuilder.UsageMode.None))
            {
                writer.Reset(BlittableJsonDocumentBuilder.UsageMode.None);

                writer.StartWriteObjectDocument();
                writer.StartWriteObject();

                writer.WritePropertyName("Type");
                writer.WriteValue(nameof(RachisEntry));
                
                writer.WritePropertyName(nameof(RachisEntry.Index));

                var index = Bits.SwapBytes(*(long*)value.Reader.Read(0, out int size));
                Debug.Assert(size == sizeof(long));
                writer.WriteValue(index);

                writer.WritePropertyName(nameof(RachisEntry.Term));
                var term = *(long*)value.Reader.Read(1, out size);
                Debug.Assert(size == sizeof(long));
                writer.WriteValue(term);

                writer.WritePropertyName(nameof(RachisEntry.Entry));
                writer.WriteEmbeddedBlittableDocument(value.Reader.Read(2, out size), size);


                writer.WritePropertyName(nameof(RachisEntry.Flags));
                var flags = *(RachisEntryFlags*)value.Reader.Read(3, out size);
                Debug.Assert(size == sizeof(RachisEntryFlags));
                writer.WriteValue(flags.ToString());


                writer.WriteObjectEnd();
                writer.FinalizeDocument();
                entry = writer.CreateReader();
            }
            return entry;
        }

        private long InitialNegotiationWithFollower()
        {
            Interlocked.Exchange(ref _followerMatchIndex, 0);
            using (_engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            {
                LogLengthNegotiation lln;
                using (context.OpenReadTransaction())
                {
                    var lastIndexEntry = _engine.GetLastEntryIndex(context);
                    lln = new LogLengthNegotiation
                    {
                        Term = _term,
                        PrevLogIndex = lastIndexEntry,
                        PrevLogTerm = _engine.GetTermForKnownExisting(context, lastIndexEntry)
                    };
                }

                UpdateLastSend("Negotiation");
                _connection.Send(context, lln);

                var llr = _connection.Read<LogLengthNegotiationResponse>(context);

                FollowerCommandsVersion = GetFollowerVersion(llr);
                _leader.PeersVersion[_tag] = FollowerCommandsVersion;
                var minimalVersion = _engine.CommandsVersionManager.GetClusterMinimalVersion(_leader.PeersVersion.Values.ToList(), _engine.MaximalVersion);
                _engine.CommandsVersionManager.SetClusterVersion(minimalVersion);

                if (_engine.Log.IsDebugEnabled)
                {
                    _engine.Log.Debug($"Got 1st LogLengthNegotiationResponse from {_tag} with term {llr.CurrentTerm:#,#;;0} " +
                                     $"({llr.MidpointIndex:#,#;;0} / {llr.MidpointTerm:#,#;;0}) {llr.Status}, version: {FollowerCommandsVersion}");
                }
                // need to negotiate
                do
                {
                    if (llr.CurrentTerm > _term)
                    {
                        // we need to abort the current leadership
                        var msg = $"{ToString()}: found election term {llr.CurrentTerm:#,#;;0} that is higher than ours {_term:#,#;;0}";
                        _engine.SetNewState(RachisState.Follower, null, _term, msg);
                        _engine.FoundAboutHigherTerm(llr.CurrentTerm, "Append entries response with higher term");
                        RachisInvalidOperationException.Throw(msg);
                    }

                    if (llr.Status == LogLengthNegotiationResponse.ResponseStatus.Acceptable)
                    {
                        if (_engine.Log.IsInfoEnabled)
                        {
                            _engine.Log.Info($"{ToString()}: {_tag} agreed on term={llr.CurrentTerm:#,#;;0} index={llr.LastLogIndex:#,#;;0}");
                        }
                        return llr.LastLogIndex;
                    }

                    if (llr.Status == LogLengthNegotiationResponse.ResponseStatus.Rejected)
                    {
                        var message = "Failed to get acceptable status from " + _tag + " because " + llr.Message;
                        if (_engine.Log.IsInfoEnabled)
                        {
                            _engine.Log.Info($"{ToString()}: {message}");
                        }
                        RachisInvalidOperationException.Throw(message);
                    }

                    UpdateLastMatchFromFollower(0);

                    using (context.OpenReadTransaction())
                    {
                        var termForMidpointIndex = _engine.GetTermFor(context, llr.MidpointIndex);
                        bool truncated = false;
                        if (termForMidpointIndex == null) //follower has this log entry but we already truncated it.
                        {
                            truncated = true;
                        }
                        else if (llr.MidpointTerm == termForMidpointIndex)
                        {
                            llr.MinIndex = Math.Min(llr.MidpointIndex + 1, llr.MaxIndex);
                        }
                        else
                        {
                            llr.MaxIndex = Math.Max(llr.MidpointIndex - 1, llr.MinIndex);
                        }
                        var midIndex = (llr.MinIndex + llr.MaxIndex) / 2;
                        var termFor = _engine.GetTermFor(context, midIndex);

                        truncated |= termFor == null;

                        if (truncated)
                        {
                            midIndex = _engine.GetFirstEntryIndex(context);
                            termFor = _engine.GetTermForKnownExisting(context, midIndex);
                        }

                        Debug.Assert(termFor != 0);
                        lln = new LogLengthNegotiation
                        {
                            Term = _term,
                            PrevLogIndex = midIndex,
                            PrevLogTerm = termFor ?? 0,
                            Truncated = truncated
                        };
                        if (_engine.Log.IsInfoEnabled)
                        {
                            _engine.Log.Info($"Sending LogLengthNegotiation to {_tag} with term {lln.Term:#,#;;0} " +
                                             $"({lln.PrevLogIndex:#,#;;0} / {lln.PrevLogTerm:#,#;;0}) - Truncated {lln.Truncated}");
                        }
                    }
                    UpdateLastSend("Negotiation 2");
                    _connection.Send(context, lln);
                    llr = _connection.Read<LogLengthNegotiationResponse>(context);
                    if (_engine.Log.IsDebugEnabled)
                    {
                        _engine.Log.Debug($"Got LogLengthNegotiationResponse from {_tag} with term {llr.CurrentTerm} " +
                                         $"({llr.MidpointIndex:#,#;;0} / {llr.MidpointTerm:#,#;;0}) {llr.Status}");
                    }
                } while (true);
            }
        }

        private static int GetFollowerVersion(LogLengthNegotiationResponse llr)
        {
            var version = llr.CommandsVersion ?? ClusterCommandsVersionManager.Base40CommandsVersion;
            if (version == 400)
                return ClusterCommandsVersionManager.Base40CommandsVersion;
            return version;
        }

        private void SendHello(ClusterOperationContext context, ClusterTopology clusterTopology)
        {
            UpdateLastSend("Hello");
            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info($"{ToString()}: sending Rachis hello to {_tag}");
            }
            _connection.Send(context, new RachisHello
            {
                TopologyId = clusterTopology.TopologyId,
                InitialMessageType = InitialMessageType.AppendEntries,
                DebugDestinationIdentifier = _tag,
                DebugSourceIdentifier = _engine.Tag,
                ElectionTimeout = (int)_engine.ElectionTimeout.TotalMilliseconds,
                SendingThread = Thread.CurrentThread.ManagedThreadId,
                DestinationUrl = _url,
                SourceUrl = _engine.Url
            });
        }

        public void Start()
        {
            UpdateLastMatchFromFollower(0);
            _followerAmbassadorLongRunningOperation =
                PoolOfThreads.GlobalRavenThreadPool.LongRunning(Run, null, ThreadNames.ForFollowerAmbassador(ToString(), _tag, $"{_term:#,#;;0}"));
        }

        public override string ToString()
        {
            return $"Follower Ambassador for {_tag} in term {_term:#,#;;0}";
        }

        public void Dispose()
        {
            _running.Lower();
            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info($"Dispose {ToString()}");
            }
            if (_followerAmbassadorLongRunningOperation != null && _followerAmbassadorLongRunningOperation.ManagedThreadId != Thread.CurrentThread.ManagedThreadId)
            {
                Volatile.Read(ref _connection)?.Dispose();

                while (_followerAmbassadorLongRunningOperation.Join(1000) == false)
                {
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info($"{ToString()}: Waited for a full second for thread {_followerAmbassadorLongRunningOperation.ManagedThreadId} ({(_followerAmbassadorLongRunningOperation.Join(0) ? "Running" : "Finished")}) to close, disposing connection and trying");
                    }
                    // the thread may have create a new connection, so need
                    // to dispose that as well

                    Volatile.Read(ref _connection)?.Dispose();
                }
            }
            _engine.InMemoryDebug.RemoveRecorderOlderThan(DateTime.UtcNow.AddMinutes(-5));
        }
    }
}
