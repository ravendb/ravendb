using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using Raven.Client.Exceptions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;
using Voron.Data.Tables;
using Voron.Global;

namespace Raven.Server.Rachis
{
    public class RachisConsensus : IDisposable
    {
        private readonly StorageEnvironmentOptions _options;
        public TransactionContextPool ContextPool { get; private set; }
        private StorageEnvironment _persistentState;
        internal readonly Logger Log;
        public RachisStateMachine StateMachine;

        public long CurrentTerm { get; private set; }
        public string TopologyId { get; private set; }

        private static readonly Slice GlobalStateSlice;
        private static readonly Slice CurrentTermSlice;
        private static readonly Slice TopologyIdSlice;
        private static readonly Slice LastAppliedSlice;
        private static readonly Slice LastCommitSlice;


        internal static readonly Slice EntriesSlice;
        internal static readonly TableSchema LogsTable;

        static RachisConsensus()
        {
            Slice.From(StorageEnvironment.LabelsContext, "GlobalState", out GlobalStateSlice);
            Slice.From(StorageEnvironment.LabelsContext, "CurrentTerm", out CurrentTermSlice);
            Slice.From(StorageEnvironment.LabelsContext, "TopologyId", out TopologyIdSlice);
            Slice.From(StorageEnvironment.LabelsContext, "LastApplied", out LastAppliedSlice);
            Slice.From(StorageEnvironment.LabelsContext, "LastCommit", out LastCommitSlice);

            Slice.From(StorageEnvironment.LabelsContext, "Entries", out EntriesSlice);

            LogsTable = new TableSchema();
            LogsTable.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
            });
        }

        public RachisConsensus(StorageEnvironmentOptions options)
        {
            _options = options;
            Log = LoggingSource.Instance.GetLogger<RachisConsensus>(options.BasePath);
        }

        public unsafe void Initialize(RachisStateMachine stateMachine)
        {
            try
            {
                StateMachine = stateMachine;
                _persistentState = new StorageEnvironment(_options);
                using (var tx = _persistentState.WriteTransaction())
                {
                    LogsTable.Create(tx, EntriesSlice, 16);

                    var state = tx.CreateTree(GlobalStateSlice);
                    if (state.State.NumberOfEntries == 0)
                    {
                        *(long*)state.DirectAdd(CurrentTermSlice, sizeof(long)) = CurrentTerm = 0;
                        state.Add(TopologyIdSlice, Array.Empty<byte>());
                        TopologyId = null;
                    }
                    else
                    {
                        var read = state.Read(CurrentTermSlice);
                        if (read == null || read.Reader.Length != sizeof(long))
                            throw new InvalidOperationException(
                                "Could not read the current term from persistent storage");
                        CurrentTerm = read.Reader.ReadLittleEndianInt64();
                        read = state.Read(TopologyIdSlice);
                        if (read == null)
                            throw new InvalidOperationException("Could not read the topology id from persistent storage");
                        TopologyId = read.Reader.ReadString(read.Reader.Length);
                    }
                }
                ContextPool = new TransactionContextPool(_persistentState);
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        public string GetDebugInformation()
        {
            // TODO: Full debug information (maching name, port, ip, etc)
            return Environment.MachineName;
        }

        public void WaitHeartbeat()
        {
            //TODO: This need to wait the random timeout
            //TODO: need to abort when shutting down / no longer leading 
            //TODO: Should explciitly be sleeping (to avoid having runnable threads)
            Thread.Sleep(250);
        }

        /// <summary>
        /// This method is expected to run for a long time (lifetime of the connection)
        /// and can never throw. We expect this to be on a separate thread
        /// </summary>
        public void AcceptNewConnection(Stream stream, TcpClient tcpClient)
        {
            RemoteConnection remoteConnection = null;
            try
            {
                remoteConnection = new RemoteConnection(stream);
                try
                {
                    RachisHello initialMessage;
                    TransactionOperationContext context;
                    using (ContextPool.AllocateOperationContext(out context))
                        initialMessage = remoteConnection.InitFollower(context);

                    if (initialMessage.TopologyId != TopologyId &&
                        string.IsNullOrEmpty(TopologyId) == false)
                    {
                        throw new InvalidOperationException(
                            $"{initialMessage.DebugSourceIdentifier} attempted to connect to us with topology id {initialMessage.TopologyId} but our topology id is already set ({TopologyId}). " +
                            $"Rejecting connection from outside our cluster, this is likely an old server trying to connect to us.");
                    }

                    switch (initialMessage.InitialMessageType)
                    {
                        case InitialMessageType.RequestVote:
                            throw new NotImplementedException("Too young to vote");
                        case InitialMessageType.AppendEntries:
                            var follower = new Follower(this,remoteConnection);
                            follower.Run();
                            break;
                        default:
                            throw new ArgumentOutOfRangeException("Uknown initial message value: " +
                                                                  initialMessage.InitialMessageType +
                                                                  ", no idea how to handle it");
                    }

                    //initialMessage.AppendEntries
                    // validate that can handle this
                    // start listening thread
                }
                catch (Exception e)
                {
                    TransactionOperationContext context;
                    using (ContextPool.AllocateOperationContext(out context))
                        remoteConnection.Send(context, e);
                }
            }
            catch (Exception e)
            {
                if (Log.IsInfoEnabled)
                {
                    Log.Info("Failed to process incoming connection", e);
                }

                try
                {
                    remoteConnection?.Dispose();
                }
                catch (Exception)
                {
                }

                try
                {
                    stream?.Dispose();
                }
                catch (Exception)
                {
                }

                try
                {
                    tcpClient?.Dispose();
                }
                catch (Exception)
                {
                }

            }
        }


        public unsafe void AppendToLog(TransactionOperationContext context, List<RachisEntry> entries)
        {
            Debug.Assert(entries.Count > 0);
            Debug.Assert(context.Transaction != null);
            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);

            long reversedEntryIndex = -1;
            Slice key;
            using (Slice.External(context.Transaction.InnerTransaction.Allocator, (byte*)&reversedEntryIndex, sizeof(long), out key))
            {
                foreach (var entry in entries)
                {
                    reversedEntryIndex = Bits.SwapBytes(entry.Index);
                    TableValueReader reader;
                    if (table.ReadByKey(key, out reader)) // already exists
                    {
                        int size;
                        var term = *(long*)reader.Read(1, out size);
                        Debug.Assert(size == sizeof(long));
                        if (term == entry.Term)
                            continue; // same, can skip

                        // we have found a divergence in the log, and we now need to trucate it from this
                        // location forward

                        do
                        {
                            table.Delete(reader.Id);
                            // move to the next id, have to swap to little endian & back to get proper
                            // behavior
                            reversedEntryIndex = Bits.SwapBytes(Bits.SwapBytes(reversedEntryIndex) + 1);
                            // now we'll find the next item to delete, and do so until we run out of items 
                            // to write
                        } while (table.ReadByKey(key, out reader));
                    }

                    table.Insert(new TableValueBuilder
                    {
                        reversedEntryIndex,
                        entry.Term,
                        {entry.Entry.BasePointer, entry.Entry.Size}
                    });
                }
            }
        }

      
        public unsafe BlittableJsonReaderObject GetEntry(TransactionOperationContext context, long index)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);
            var reversedIndex = Bits.SwapBytes(index);
            Slice key;
            using (Slice.External(context.Allocator, (byte*)&reversedIndex, sizeof(long), out key))
            {
                TableValueReader reader;
                if (table.ReadByKey(key, out reader) == false)
                    return null;
                int size;
                var ptr = reader.Read(2, out size);
                return new BlittableJsonReaderObject(ptr, size, context);
            }
        }

        public long GetLastAppliedIndex(TransactionOperationContext context)
        {
            Debug.Assert(context.Transaction != null);

            var state = context.Transaction.InnerTransaction.ReadTree(GlobalStateSlice);
            var read = state.Read(LastAppliedSlice);
            if (read == null)
                return 0;
            return read.Reader.ReadLittleEndianInt64();
        }

        public long GetLastCommitIndex(TransactionOperationContext context)
        {
            Debug.Assert(context.Transaction != null);

            var state = context.Transaction.InnerTransaction.ReadTree(GlobalStateSlice);
            var read = state.Read(LastAppliedSlice);
            if (read == null)
                return 0;
            return read.Reader.ReadLittleEndianInt64();
        }


        public unsafe void SetLastAppliedIndex(TransactionOperationContext context, long value)
        {
            Debug.Assert(context.Transaction != null);

            var state = context.Transaction.InnerTransaction.ReadTree(GlobalStateSlice);
            var read = state.Read(LastAppliedSlice);
            if (read != null)
            {
                var oldValue = read.Reader.ReadLittleEndianInt64();
                if (oldValue >= value)
                    throw new InvalidOperationException(
                        $"Cannot reduce the last applied index (is {oldValue} but was requested to reduce to {value})");
            }

            *(long*)state.DirectAdd(LastAppliedSlice, sizeof(long)) = value;
        }

        public unsafe Tuple<long, long> GetLogEntriesRange(TransactionOperationContext context)
        {
            Debug.Assert(context.Transaction != null);

            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);
            TableValueReader reader;
            if (table.SeekOnePrimaryKey(Slices.AfterAllKeys, out reader) == false)
                return Tuple.Create(0L, 0L);
            int size;
            var max = Bits.SwapBytes(*(long*)reader.Read(0, out size));
            Debug.Assert(size == sizeof(long));
            if (table.SeekOnePrimaryKey(Slices.BeforeAllKeys, out reader) == false)
                return Tuple.Create(0L, 0L);
            var min = Bits.SwapBytes(*(long*)reader.Read(0, out size));
            Debug.Assert(size == sizeof(long));

            return Tuple.Create(min, max);
        }

        public long GetTermForKnownExisting(TransactionOperationContext context, long index)
        {
            var termFor = GetTermFor(context, index);
            if (termFor == null)
                throw new InvalidOperationException("Expected the index " + index +
                                                    " to have a term in the entries, but got null");
            return termFor.Value;
        }

        public long? GetTermFor(long index)
        {
            TransactionOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                return GetTermFor(context, index);
            }
        }

        public static unsafe long? GetTermFor(TransactionOperationContext context, long index)
        {
            Debug.Assert(context.Transaction != null);
            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);
            var reversedIndex = Bits.SwapBytes(index);
            Slice key;
            using (Slice.External(context.Transaction.InnerTransaction.Allocator, (byte*)&reversedIndex, sizeof(long), out key))
            {
                TableValueReader reader;
                if (table.ReadByKey(key, out reader) == false)
                    return null;
                int size;
                var term = *(long*)reader.Read(1, out size);
                Debug.Assert(size == sizeof(long));
                return term;
            }
        }

        public unsafe void UpdateCurrentTerm(long term)
        {
            if (term == CurrentTerm)
                return;

            // TODO
            // if leader, need to become follower

            TransactionOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                using (var tx = context.OpenWriteTransaction())
                {
                    // we check it here again because now we are under the tx lock, so we can't get into concurrency issues

                    if (term < CurrentTerm)
                        throw new ConcurrencyException($"The current term {CurrentTerm} is larger than {term}, aborting change");

                    var state = tx.InnerTransaction.CreateTree(GlobalStateSlice);
                    *(long*)state.DirectAdd(CurrentTermSlice, sizeof(long)) = term;

                    CurrentTerm = term;

                    tx.Commit();
                }
            }
        }


        public void Dispose()
        {
            StateMachine?.Dispose();
            ContextPool?.Dispose();
            _persistentState?.Dispose();
            _options?.Dispose();
        }
    }
}