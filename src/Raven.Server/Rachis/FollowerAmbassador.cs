using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Voron;
using Voron.Data;
using Voron.Data.Tables;
using Voron.Global;

namespace Raven.Server.Rachis
{
    public class FollowerAmbassador : IDisposable
    {
        private readonly RachisConsensus _engine;
        private readonly Leader _leader;
        private ManualResetEvent _wakeLeader;
        private readonly string _url;
        private readonly string _apiKey;

        public string Status;

        private long _followerMatchIndex;
        private long _lastReplyFromFollower;
        private Thread _thread;
        private RemoteConnection _connection;

        public string Url => _url;

        public long FollowerMatchIndex => Interlocked.Read(ref _followerMatchIndex);

        public DateTime LastReplyFromFollower => new DateTime(Interlocked.Read(ref _lastReplyFromFollower));

        private void UpdateLastMatchFromFollower(long newVal)
        {
            Interlocked.Exchange(ref _lastReplyFromFollower, DateTime.UtcNow.Ticks);
            Interlocked.Exchange(ref _followerMatchIndex, newVal);
            _wakeLeader.Set();
        }

        public FollowerAmbassador(RachisConsensus engine, Leader leader, ManualResetEvent wakeLeader, string url, string apiKey)
        {
            _engine = engine;
            _leader = leader;
            _wakeLeader = wakeLeader;
            _url = url;
            _apiKey = apiKey;
            Status = "Started";
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
        private unsafe void Run()
        {
            try
            {
                while (_leader.Running)
                {
                    Stream stream = null;
                    try
                    {
                        try
                        {
                            stream = _engine.ConenctToPeer(_url, _apiKey);
                        }
                        catch (Exception e)
                        {
                            Status = "Failed - " + e.Message;
                            if (_engine.Log.IsInfoEnabled)
                            {
                                _engine.Log.Info("Failed to connect to remote follower: " + _url, e);
                            }
                            // wait a bit
                            _leader.WaitForNewEntries().Wait(_engine.ElectionTimeoutMs / 2);
                            continue; // we'll retry connecting
                        }
                        Status = "Connected";
                        _connection = new RemoteConnection(_url,stream);
                        using (_connection)
                        {
                            _engine.AppendStateDisposable(_leader, _connection);
                            var matchIndex = InitialNegotiationWithFollower();
                            if (matchIndex == null)
                                return;
                            UpdateLastMatchFromFollower(matchIndex.Value);
                            SendSnapshot(stream);

                            var entries = new List<BlittableJsonReaderObject>();
                            while (_leader.Running)
                            {
                                // TODO: how to close
                                entries.Clear();
                                TransactionOperationContext context;
                                using (_engine.ContextPool.AllocateOperationContext(out context))
                                {
                                    AppendEntries appendEntries;
                                    using (context.OpenReadTransaction())
                                    {
                                        var table = context.Transaction.InnerTransaction.OpenTable(
                                            RachisConsensus.LogsTable,
                                            RachisConsensus.EntriesSlice);

                                        var reveredNextIndex = Bits.SwapBytes(_followerMatchIndex + 1);
                                        Slice key;
                                        using (
                                            Slice.External(context.Allocator, (byte*)&reveredNextIndex, sizeof(long),
                                                out key))
                                        {
                                            long totalSize = 0;
                                            foreach (var value in table.SeekByPrimaryKey(key))
                                            {
                                                var entry = BuildRachisEntryToSend(context, value);
                                                entries.Add(entry);
                                                totalSize += entry.Size;
                                                if (totalSize > Constants.Size.Megabyte)
                                                    break; // TODO: Configurable?
                                            }

                                            appendEntries = new AppendEntries
                                            {
                                                EntriesCount = entries.Count,
                                                LeaderCommit = _engine.GetLastCommitIndex(context),
                                                Term = _engine.CurrentTerm,
                                                TruncateLogBefore = _leader.LowestIndexInEntireCluster,
                                                PrevLogTerm = _engine.GetTermFor(context,_followerMatchIndex) ?? 0,
                                                PrevLogIndex = _followerMatchIndex
                                            };
                                        }
                                    }

                                    // out of the tx, we can do network calls

                                    _connection.Send(context, appendEntries, entries);
                                    var aer = _connection.Read<AppendEntriesResponse>(context);

                                    if (aer.Success == false)
                                    {
                                        // shouldn't happen, the connection should be aborted if this is the case, but still
                                        var msg =
                                            "A negative Append Entries Response after the connection has been established shouldn't happen. Message: " +
                                            aer.Message;
                                        if (_engine.Log.IsInfoEnabled)
                                        {
                                            _engine.Log.Info("BUG? " + msg);
                                        }
                                        throw new InvalidOperationException(msg);
                                    }
                                    Debug.Assert(aer.CurrentTerm == _engine.CurrentTerm);
                                    UpdateLastMatchFromFollower(aer.LastLogIndex);
                                }
                                var task = _leader.WaitForNewEntries();
                                using (_engine.ContextPool.AllocateOperationContext(out context))
                                using (context.OpenReadTransaction())
                                {
                                    if (_engine.GetLastEntryIndex(context) != _followerMatchIndex)
                                        continue; // instead of waiting, we have new entries, start immediately
                                }
                                // either we have new entries to send, or we waited for long enough 
                                // to send another heartbeat
                                task.Wait(_engine.ElectionTimeoutMs / 3);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Status = "Failed - " + e.Message;
                        if (_engine.Log.IsInfoEnabled)
                        {
                            _engine.Log.Info("Failed to talk to remote follower: " + _url, e);
                        }
                        _leader.WaitForNewEntries().Wait(_engine.ElectionTimeoutMs / 2);
                    }
                    finally
                    {
                        stream?.Dispose();
                        Status = "Disconnected";
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Status = "Closed";
            }
            catch (ObjectDisposedException)
            {
                Status = "Closed";
            }
            catch (AggregateException ae)
                when (ae.InnerException is OperationCanceledException || ae.InnerException is ObjectDisposedException)
            {
                Status = "Closed";
            }
            catch (Exception e)
            {
                Status = "Failed - " + e.Message;
                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info("Failed to talk to remote follower: " + _url, e);
                }
            }
        }

        private void SendSnapshot(Stream stream)
        {
            TransactionOperationContext context;
            using (_engine.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                var earliestIndexEtry = _engine.GetFirstEntryIndex(context);
                if (_followerMatchIndex >= earliestIndexEtry)
                {
                    // we don't need a snapshot, so just send updated topology
                    _connection.Send(context, new InstallSnapshot
                    {
                        LastIncludedIndex = 0,
                        LastIncludedTerm = 0,
                        Topology = _engine.GetTopologyRaw(context),
                    });
                    using (var binaryWriter = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true))
                    {
                        binaryWriter.Write((int) RootObjectType.None);
                    }
                }
                else
                {
                    long index;
                    long term;
                    _engine.GetLastCommitIndex(context, out index, out term);

                    // we make sure that we routinely update LastReplyFromFollower here
                    // so we'll not leave the leader thinking we abandoned it
                    UpdateLastMatchFromFollower(_followerMatchIndex);

                    _connection.Send(context, new InstallSnapshot
                    {
                        LastIncludedIndex = index,
                        LastIncludedTerm = term,
                        Topology = _engine.GetTopologyRaw(context),
                    });

                    WriteSnapshotToFile(context, new BufferedStream(stream));

                    UpdateLastMatchFromFollower(_followerMatchIndex);
                }

                while (true)
                {
                    var aer = _connection.Read<InstallSnapshotResponse>(context);
                    if (aer.Done)
                    {
                        UpdateLastMatchFromFollower(aer.LastLogIndex);
                        break;
                    }
                    UpdateLastMatchFromFollower(_followerMatchIndex);
                }
            }
        }

        private void MaybeNotifyLeaderThatWeAreSillAlive(long count, Stopwatch sp)
        {
            if (count % 100 != 0)
                return;

            if (sp.ElapsedMilliseconds <= _engine.ElectionTimeoutMs / 2)
                return;

            sp.Restart();

            UpdateLastMatchFromFollower(_followerMatchIndex);
        }

        private unsafe void WriteSnapshotToFile(TransactionOperationContext context, Stream dest)
        {
            var copier = new UnmanagedMemoryToStream(dest);
            var sp = Stopwatch.StartNew();
            long count = 0;

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

                        MaybeNotifyLeaderThatWeAreSillAlive(count++, sp);

                        var currentTreeKey = rootIterator.CurrentKey;

                        binaryWriter.Write((int)rootObjectType);
                        binaryWriter.Write(currentTreeKey.Size);
                        copier.Copy(currentTreeKey.Content.Ptr, currentTreeKey.Size);

                        switch (rootObjectType)
                        {
                            case RootObjectType.VariableSizeTree:
                                var tree = txr.ReadTree(currentTreeKey);
                                binaryWriter.Write(tree.State.NumberOfEntries);

                                using (var treeIterator = tree.Iterate(false))
                                {
                                    if (treeIterator.Seek(Slices.BeforeAllKeys))
                                    {
                                        do
                                        {
                                            var currentTreeValueKey = treeIterator.CurrentKey;
                                            binaryWriter.Write(currentTreeValueKey.Size);
                                            copier.Copy(currentTreeValueKey.Content.Ptr, currentTreeValueKey.Size);
                                            var reader = treeIterator.CreateReaderForCurrent();
                                            binaryWriter.Write(reader.Length);
                                            copier.Copy(reader.Base, reader.Length);
                                            MaybeNotifyLeaderThatWeAreSillAlive(count++, sp);
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
                                foreach (var holder in inputTable.SeekByPrimaryKey(Slices.BeforeAllKeys))
                                {
                                    MaybeNotifyLeaderThatWeAreSillAlive(count++, sp);
                                    binaryWriter.Write(holder.Reader.Size);
                                    copier.Copy(holder.Reader.Pointer, holder.Reader.Size);
                                }
                                break;
                            default:
                                throw new ArgumentOutOfRangeException(nameof(rootObjectType), rootObjectType + " " + rootIterator.CurrentKey);
                        }

                    } while (rootIterator.MoveNext());
                }
                binaryWriter.Write((int)RootObjectType.None);
            }
            MaybeNotifyLeaderThatWeAreSillAlive(0, sp);

            dest.Flush();
        }

        private unsafe class UnmanagedMemoryToStream
        {
            private readonly byte[] _buffer = new byte[1024];

            public byte[] Buffer => _buffer;

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
                        var count = Math.Min(size, _buffer.Length);
                        Memory.Copy(pBuffer, ptr, count);
                        _stream.Write(_buffer, 0, count);
                        ptr += count;
                        size -= count;
                    }
                }
            }
        }


        private static unsafe BlittableJsonReaderObject BuildRachisEntryToSend(TransactionOperationContext context,
            Table.TableValueHolder value)
        {
            BlittableJsonReaderObject entry;
            using (var writer =
                new ManualBlittalbeJsonDocumentBuilder<UnmanagedWriteBuffer>(
                    context, BlittableJsonDocumentBuilder.UsageMode.None))
            {
                writer.Reset(BlittableJsonDocumentBuilder.UsageMode.None);

                writer.StartWriteObjectDocument();
                writer.StartWriteObject();

                writer.WritePropertyName("Type");
                writer.WriteValue(nameof(RachisEntry));

                writer.WritePropertyName(nameof(RachisEntry.Index));

                int size;
                var index = Bits.SwapBytes(*(long*)value.Reader.Read(0, out size));
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

        private long? InitialNegotiationWithFollower()
        {
            TransactionOperationContext context;
            using (_engine.ContextPool.AllocateOperationContext(out context))
            {
                ClusterTopology clusterTopology;
                AppendEntries appendEntries;
                using (context.OpenReadTransaction())
                {
                    clusterTopology = _engine.GetTopology(context);
                    var lastIndexEntry = _engine.GetLastEntryIndex(context);
                    appendEntries = new AppendEntries
                    {
                        EntriesCount = 0,
                        Term = _engine.CurrentTerm,
                        LeaderCommit = _engine.GetLastCommitIndex(context),
                        PrevLogIndex = lastIndexEntry,
                        PrevLogTerm = _engine.GetTermForKnownExisting(context, lastIndexEntry),
                    };
                }

                _connection.Send(context, new RachisHello
                {
                    TopologyId = clusterTopology.TopologyId,
                    InitialMessageType = InitialMessageType.AppendEntries,
                    DebugSourceIdentifier = _engine.GetDebugInformation()
                });


                _connection.Send(context, appendEntries);

                var aer = _connection.Read<AppendEntriesResponse>(context);

                // need to negotiate
                do
                {
                    if (aer.CurrentTerm > _engine.CurrentTerm)
                    {
                        // we need to abort the current leadership
                        _engine.SetNewState(RachisConsensus.State.Follower, null);
                        _engine.FoundAboutHigherTerm(aer.CurrentTerm);
                        return null;
                    }

                    if (aer.Success)
                    {
                        return aer.LastLogIndex;
                    }

                    if (aer.Negotiation == null)
                        throw new InvalidOperationException("BUG: We didn't get a success on first AppendEntries to peer " +
                                                            _url + ", the term match but there is no negotiation");

                    using (context.OpenReadTransaction())
                    {
                        if (aer.Negotiation.MidpointTerm ==
                            _engine.GetTermForKnownExisting(context, aer.Negotiation.MidpointIndex))// we know that we have longer / equal log tot the follower
                        {
                            aer.Negotiation.MinIndex = aer.Negotiation.MidpointIndex + 1;
                        }
                        else
                        {
                            aer.Negotiation.MaxIndex = aer.Negotiation.MidpointIndex - 1;
                        }
                        var midIndex = (aer.Negotiation.MinIndex + aer.Negotiation.MaxIndex) / 2;
                        appendEntries = new AppendEntries
                        {
                            EntriesCount = 0,
                            Term = _engine.CurrentTerm,
                            LeaderCommit = _engine.GetLastCommitIndex(context),
                            PrevLogIndex = midIndex,
                            PrevLogTerm = _engine.GetTermForKnownExisting(context, midIndex),
                        };
                    }
                    _connection.Send(context, appendEntries);
                    aer = _connection.Read<AppendEntriesResponse>(context);
                } while (aer.Success == false);

                return aer.LastLogIndex;
            }
        }

        public void Start()
        {
            _thread = new Thread(Run)
            {
                Name = "Follower Ambasaddor for " + _url,
                IsBackground = true
            };
            _thread.Start();
        }

        public void Dispose()
        {
            _connection?.Dispose();

            if (_thread != null && _thread.ManagedThreadId != Thread.CurrentThread.ManagedThreadId)
                _thread.Join();
        }
    }
}