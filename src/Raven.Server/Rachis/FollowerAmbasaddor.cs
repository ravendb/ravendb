using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Sparrow.Json;
using Voron;
using Voron.Data.Tables;
using Voron.Global;

namespace Raven.Server.Rachis
{
    public class FollowerAmbasaddor
    {
        private readonly RachisConsensus _engine;
        private readonly ManualResetEventSlim _wakeLeader;
        private readonly Func<Stream> _conenctToFollower;
        private readonly string _debugFollower;

        private long _followerNextIndex;// this is only accessed by this thread
        private long _followerMatchIndex;
        private long _lastReplyFromFollower;


        public long FollowerMatchIndex => Interlocked.Read(ref _followerMatchIndex);

        public DateTime LastReplyFromFollower => new DateTime(Interlocked.Read(ref _lastReplyFromFollower));

        private void UpdateLastMatchFromFollower(long newVal)
        {
            Interlocked.Exchange(ref _lastReplyFromFollower, DateTime.UtcNow.Ticks);
            var oldVal = Interlocked.Exchange(ref _followerMatchIndex, newVal);
            if (oldVal != newVal)
                _wakeLeader.Set();
        }

        public FollowerAmbasaddor(RachisConsensus engine, ManualResetEventSlim wakeLeader, Func<Stream> conenctToFollower, string debugFollower)
        {
            _engine = engine;
            _wakeLeader = wakeLeader;
            _conenctToFollower = conenctToFollower;
            _debugFollower = debugFollower;
        }

        /// <summary>
        /// This method is expected to run for a long time (as long as we are the leader)
        /// it is responsible for talking to the remote follower and maintaining its state.
        /// This can never throw, and will run on its own thread.
        /// </summary>
        public unsafe void FollowerAmbassador()
        {
            try
            {
                while (true)
                {
                    //TODO: need a way to shut this down when we are no longer leader / shutting down
                    Stream stream = null;
                    try
                    {
                        try
                        {
                            stream = _conenctToFollower();
                        }
                        catch (Exception e)
                        {
                            if (_engine.Log.IsInfoEnabled)
                            {
                                _engine.Log.Info("Failed to connect to remote follower: " + _debugFollower, e);
                            }
                            _engine.WaitHeartbeat();
                            continue; // we'll retry connecting
                        }

                        using (var connection = new RemoteConnection(stream))
                        {
                            var matchIndex = InitialNegotiationWithFollower(_debugFollower, connection);
                            if (matchIndex == null)
                                return;

                            UpdateLastMatchFromFollower(matchIndex.Value);

                            TransactionOperationContext context;
                            using (_engine.ContextPool.AllocateOperationContext(out context))
                            {
                                //TODO: implement this
                                connection.Send(context, new InstallSnapshot
                                {
                                    LastIncludedIndex = -1,
                                    LastIncludedTerm = -1,
                                    Topology = -1,//TODO: fake, just to remember doing this
                                    SnapshotSize = 0
                                });

                                //TODO: make sure that we routinely update LastReplyFromFollower here
                            }

                            var entries = new List<BlittableJsonReaderObject>();
                            while (true)
                            {
                                // TODO: how to close
                                entries.Clear();
                                using (_engine.ContextPool.AllocateOperationContext(out context))
                                {
                                    AppendEntries appendEntries;
                                    using (context.OpenReadTransaction())
                                    {
                                        var table = context.Transaction.InnerTransaction.OpenTable(RachisConsensus.LogsTable, RachisConsensus.EntriesSlice);

                                        var reveredNextIndex = Bits.SwapBytes(_followerNextIndex);
                                        Slice key;
                                        using (Slice.External(context.Allocator, (byte*)&reveredNextIndex, sizeof(long),
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
                                                HasTopologyChange = false, // TODO: figure this out
                                                EntriesCount = entries.Count,
                                                LeaderCommit = _engine.GetLastCommitIndex(context),
                                                Term = _engine.CurrentTerm,
                                                //TODO: figure out what to do if we have a snapshot that removed all 
                                                //TODO: the entries
                                                PrevLogTerm = _engine.GetTermFor(_followerNextIndex - 1) ?? 0,
                                                PrevLogIndex = _followerNextIndex - 1
                                            };
                                        }
                                    }

                                    // out of the tx, we can do network calls

                                    connection.Send(context, appendEntries, entries);
                                    var aer = connection.Read<AppendEntriesResponse>(context);

                                    if (aer.Success == false)
                                    {
                                        // shouldn't happen, the connection should be aborted if this is the case, but still
                                        var msg = "A negative Append Entries Response after the connection has been established shouldn't happen. Message: " +
                                                  aer.Message;
                                        if (_engine.Log.IsInfoEnabled)
                                        {
                                            _engine.Log.Info("BUG? " + msg);
                                        }
                                        throw new InvalidOperationException(msg);
                                    }
                                    Debug.Assert(aer.CurrentTerm == _engine.CurrentTerm);
                                    _followerNextIndex = aer.LastLogIndex + 1;
                                    UpdateLastMatchFromFollower(aer.LastLogIndex);
                                }

                                _engine.WaitHeartbeat();
                            }
                        }
                    }
                    finally
                    {
                        stream?.Dispose();
                    }
                }
            }
            catch (Exception e)
            {
                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info("Failed to talk to remote follower: " + _debugFollower, e);
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
                writer.StartWriteObjectDocument();
                writer.StartWriteObject();

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


                writer.WriteObjectEnd();
                writer.FinalizeDocument();
                entry = writer.CreateReader();
            }
            return entry;
        }

        private long? InitialNegotiationWithFollower(string debugFollower, RemoteConnection connection)
        {
            TransactionOperationContext context;
            using (_engine.ContextPool.AllocateOperationContext(out context))
            {
                AppendEntries appendEntries;
                using (context.OpenReadTransaction())
                {
                    var range = _engine.GetLogEntriesRange(context);
                    appendEntries = new AppendEntries
                    {
                        EntriesCount = 0,
                        HasTopologyChange = false,
                        Term = _engine.CurrentTerm,
                        LeaderCommit = _engine.GetLastCommitIndex(context),
                        PrevLogIndex = range.Item2,
                        PrevLogTerm = _engine.GetTermForKnownExisting(context, range.Item2),
                    };
                }

                connection.Send(context, new RachisHello
                {
                    TopologyId = _engine.TopologyId,
                    InitialMessageType = InitialMessageType.AppendEntries,
                    DebugSourceIdentifier = _engine.GetDebugInformation()
                });


                connection.Send(context, appendEntries);

                var aer = connection.Read<AppendEntriesResponse>(context);
                if (aer.CurrentTerm != _engine.CurrentTerm)
                {
                    //TODO: Step down as leader
                    Debug.Assert(false, "TODO: Step down as leader");
                    return null;
                }

                if (aer.Success)
                {
                    _followerNextIndex = aer.LastLogIndex + 1;
                    return aer.LastLogIndex;
                }

                if (aer.Negotiation == null)
                    throw new InvalidOperationException("BUG: We didn't get a success on first AppendEntries to peer " +
                                                        debugFollower + ", the term match but there is no negotiation");

                // need to negotiate
                do
                {
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
                            HasTopologyChange = false,
                            Term = _engine.CurrentTerm,
                            LeaderCommit = _engine.GetLastCommitIndex(context),
                            PrevLogIndex = midIndex,
                            PrevLogTerm = _engine.GetTermForKnownExisting(context, midIndex),
                        };
                    }
                    connection.Send(context, appendEntries);
                    aer = connection.Read<AppendEntriesResponse>(context);
                } while (aer.Success == false);

                _followerNextIndex = aer.LastLogIndex + 1;
                return aer.LastLogIndex;
            }
        }

    }
}