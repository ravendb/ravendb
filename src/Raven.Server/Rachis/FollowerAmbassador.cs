using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Json;
using Voron;
using Voron.Data.Tables;
using Voron.Global;

namespace Raven.Server.Rachis
{
    public class FollowerAmbassador : IDisposable
    {
        private readonly RachisConsensus _engine;
        private readonly Leader _leader;
        private readonly ManualResetEvent _wakeLeader;
        private readonly string _url;
        private readonly string _apiKey;

        public string Status;

        private long _followerNextIndex;// this is only accessed by this thread
        private long _followerMatchIndex;
        private long _lastReplyFromFollower;
        private Thread _thread;

        public string Url => _url;

        public long FollowerMatchIndex => Interlocked.Read(ref _followerMatchIndex);

        public DateTime LastReplyFromFollower => new DateTime(Interlocked.Read(ref _lastReplyFromFollower));

        private void UpdateLastMatchFromFollower(long newVal)
        {
            Interlocked.Exchange(ref _lastReplyFromFollower, DateTime.UtcNow.Ticks);
            var oldVal = Interlocked.Exchange(ref _followerMatchIndex, newVal);
            if (oldVal != newVal)
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
                            _leader.WaitForNewEntries().Wait(_engine.ElectionTimeoutMs/2);
                            continue; // we'll retry connecting
                        }
                        Status = "Connected";
                        using (var connection = new RemoteConnection(stream))
                        {
                            var matchIndex = InitialNegotiationWithFollower(connection);
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

                                var aer = connection.Read<AppendEntriesResponse>(context);
                                if (aer.Success == false)
                                {
                                    throw new InvalidOperationException($"Unable to install snapshot on {_url} because {aer.Message}");
                                }

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
                                var task = _leader.WaitForNewEntries();
                                using (_engine.ContextPool.AllocateOperationContext(out context))
                                using (context.OpenReadTransaction())
                                {
                                    if (_engine.GetLastEntryIndex(context) != _followerMatchIndex)
                                        continue;// instead of waiting, we have new entries, start immediately
                                }
                                // either we have new entries to send, or we waited for long enough 
                                // to send another heartbeat
                                task.Wait(_engine.ElectionTimeoutMs/3);
                            }
                        }
                    }
                    finally
                    {
                        stream?.Dispose();
                        Status = "Disconnected";
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

        private long? InitialNegotiationWithFollower(RemoteConnection connection)
        {
            TransactionOperationContext context;
            using (_engine.ContextPool.AllocateOperationContext(out context))
            {
                ClusterTopology clusterTopology;
                AppendEntries appendEntries;
                using (context.OpenReadTransaction())
                {
                    clusterTopology = _engine.GetTopology(context);
                    var range = _engine.GetLogEntriesRange(context);
                    appendEntries = new AppendEntries
                    {
                        EntriesCount = 0,
                        Term = _engine.CurrentTerm,
                        LeaderCommit = _engine.GetLastCommitIndex(context),
                        PrevLogIndex = range.Item2,
                        PrevLogTerm = _engine.GetTermForKnownExisting(context, range.Item2),
                    };
                }

                connection.Send(context, new RachisHello
                {
                    TopologyId = clusterTopology.TopologyId,
                    InitialMessageType = InitialMessageType.AppendEntries,
                    DebugSourceIdentifier = _engine.GetDebugInformation()
                });


                connection.Send(context, appendEntries);

                var aer = connection.Read<AppendEntriesResponse>(context);
               
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
                        _followerNextIndex = aer.LastLogIndex + 1;
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
                    connection.Send(context, appendEntries);
                    aer = connection.Read<AppendEntriesResponse>(context);
                } while (aer.Success == false);

                _followerNextIndex = aer.LastLogIndex + 1;
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
            //TODO: shutdown notification of some kind?
            if (_thread != null && _thread.ManagedThreadId != Thread.CurrentThread.ManagedThreadId)
                _thread.Join();
        }
    }
}