using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Rachis
{
    public class RachisLogHistory
    {
        internal static readonly Slice LogHistorySlice;
        private static readonly Slice LogHistoryIndexSlice;
        private static readonly Slice LogHistoryDateTimeSlice;
        private static readonly TableSchema LogHistoryTable;

        private int _logHistoryMaxEntries;

        private Logger _log;

        public enum LogHistoryColumn
        {
            Guid, // string
            Index, // long
            Ticks, // long
            Term, // long
            CommittedTerm,
            Type, // string
            State, // byte -> 1 - appended, 2 - committed
            Result, // blittable
            ExceptionType, // string
            ExceptionMessage // string
        }

        private enum HistoryStatus : byte
        {
            None,
            Appended,
            Committed
        }

        private long _lastTicks;
        private long GetUniqueTicks(Transaction transaction)
        {
            if (transaction.IsWriteTransaction == false)
                throw new InvalidOperationException("You can acquire unique tick only under write tx.");

            var ticks = DateTime.UtcNow.Ticks;

            if (ticks <= _lastTicks)
            {
                ticks = _lastTicks + 1;
            }
            _lastTicks = ticks;

            return ticks;
        }

        static RachisLogHistory()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "LogHistory", out LogHistorySlice);
                Slice.From(ctx, "LogHistoryIndex", out LogHistoryIndexSlice);
                Slice.From(ctx, "LogHistoryDateTime", out LogHistoryDateTimeSlice);
            }
            
            LogHistoryTable = new TableSchema();
            LogHistoryTable.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)LogHistoryColumn.Guid,
            });

            LogHistoryTable.DefineIndex(new TableSchema.SchemaIndexDef
            {
                Name = LogHistoryIndexSlice,
                StartIndex = (int)LogHistoryColumn.Index,
                Count = 1
            });

            LogHistoryTable.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                Name = LogHistoryDateTimeSlice,
                StartIndex = (int)LogHistoryColumn.Ticks
            });
        }

        public void Initialize(RavenTransaction tx, RavenConfiguration configuration, Logger log)
        {
            _logHistoryMaxEntries = configuration.Cluster.LogHistoryMaxEntries;
            LogHistoryTable.Create(tx.InnerTransaction, RachisLogHistory.LogHistorySlice, 16);
            _log = log;
        }


        public string GetGuidFromCommand(BlittableJsonReaderObject cmd)
        {
            if (cmd.TryGet(nameof(CommandBase.UniqueRequestId), out string guid))
                return guid;

            return null;
        }

        public string GetTypeFromCommand(BlittableJsonReaderObject cmd)
        {
            if (cmd.TryGet("Type", out string type) == false)
            {
                throw new RachisApplyException("Cannot execute command, wrong format");
            }

            return type;
        }

        public void InsertHistoryLog(ClusterOperationContext context, long index, long term, BlittableJsonReaderObject cmd)
        {
            if (HasHistoryLog(context, cmd, out _, out _, out _))
            {
                return;
            }

            var guid = GetGuidFromCommand(cmd);
            if (guid == null) // shouldn't happened in new cluster version!
                return;

            if (guid == RaftIdGenerator.DontCareId)
                return;

            var table = context.Transaction.InnerTransaction.OpenTable(LogHistoryTable, LogHistorySlice);
            var type = GetTypeFromCommand(cmd);

            using (Slice.From(context.Allocator, guid, out var guidSlice))
            using (Slice.From(context.Allocator, type, out var typeSlice))
            using (table.Allocate(out TableValueBuilder tvb))
            {
                tvb.Add(guidSlice);
                tvb.Add(Bits.SwapBytes(index));
                tvb.Add(Bits.SwapBytes(GetUniqueTicks(context.Transaction.InnerTransaction)));
                tvb.Add(term);
                tvb.Add(0L);
                tvb.Add(typeSlice);
                tvb.Add((byte)HistoryStatus.Appended);
                tvb.Add(Slices.Empty); // result
                tvb.Add(Slices.Empty); // exception type
                tvb.Add(Slices.Empty); // exception message
                table.Set(tvb);
            }

            if (table.NumberOfEntries > _logHistoryMaxEntries)
            {
                var reader = table.SeekOneForwardFromPrefix(LogHistoryTable.Indexes[LogHistoryIndexSlice], Slices.BeforeAllKeys);
                table.Delete(reader.Reader.Id);
            }
        }

        public void UpdateHistoryLog(ClusterOperationContext context, long index, long term, BlittableJsonReaderObject cmd, object result, Exception exception)
        {
            var guid = GetGuidFromCommand(cmd);
            if (guid == null) // shouldn't happened in new cluster version!
                return;
            if (guid == RaftIdGenerator.DontCareId)
                return;

            var type = GetTypeFromCommand(cmd);

            UpdateInternal(context, guid, type, index, term, HistoryStatus.Committed, result, exception);
        }

        private unsafe void UpdateInternal(ClusterOperationContext context, string guid, string type, long index, long term, HistoryStatus status, object result, Exception exception)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(LogHistoryTable, LogHistorySlice);

            TableValueReader reader;
            using (Slice.From(context.Allocator, guid, out var guidSlice))
            {
                if (table.ReadByKey(guidSlice, out reader) == false)
                    return;
            }

            if (TypeConverter.IsSupportedType(result) == false)
            {
                throw new RachisApplyException("We don't support type " + result.GetType().FullName + ".");
            }

            using (Slice.From(context.Allocator, guid, out var guidSlice))
            using (Slice.From(context.Allocator, type, out var typeSlice))
            using (table.Allocate(out TableValueBuilder tvb))
            {
                tvb.Add(guidSlice);
                tvb.Add(Bits.SwapBytes(index));
                tvb.Add(Bits.SwapBytes(GetUniqueTicks(context.Transaction.InnerTransaction)));
                tvb.Add(*(long*)reader.Read((int)(LogHistoryColumn.Term), out _));
                tvb.Add(term);
                tvb.Add(typeSlice);
                tvb.Add((byte)status);
                if (result == null)
                {
                    tvb.Add(Slices.Empty);
                }
                else
                {
                    var blittableResult = context.ReadObject(new DynamicJsonValue { ["Result"] = result }, "set-history-result");
                    tvb.Add(blittableResult.BasePointer, blittableResult.Size);
                }

                if (exception == null)
                {
                    tvb.Add(Slices.Empty);
                    tvb.Add(Slices.Empty);
                }
                else
                {
                    var exceptionType = context.GetLazyString(exception.GetType().AssemblyQualifiedName);
                    var exceptionMsg = context.GetLazyString(exception.ToString());
                    tvb.Add(exceptionType.Buffer, exceptionType.Size);
                    tvb.Add(exceptionMsg.Buffer, exceptionMsg.Size);
                }

                table.Set(tvb);
            }
        }

        private class HistoryLogEntry
        {
            public string Guid;
            public long Index;
            public long Term;
            public string Type;
            public HistoryStatus Status;
        }

        public unsafe void CancelHistoryEntriesFrom(ClusterOperationContext context, long from, long term, string msg)
        {
            var reversedIndex = Bits.SwapBytes(from);

            var table = context.Transaction.InnerTransaction.OpenTable(LogHistoryTable, LogHistorySlice);
            using (Slice.External(context.Transaction.InnerTransaction.Allocator, (byte*)&reversedIndex, sizeof(long), out Slice key))
            {
                var results = table.SeekForwardFrom(LogHistoryTable.Indexes[LogHistoryIndexSlice], key, 0);
                var toCancel = new List<HistoryLogEntry>();
                foreach (var seekResult in results)
                {
                    var entryHolder = seekResult.Result;
                    var entryTerm = ReadTerm(entryHolder);

                    if (entryTerm == term)
                        continue;

                    var entry = new HistoryLogEntry
                    {
                        Guid = ReadGuid(entryHolder),
                        Type = ReadType(entryHolder),
                        Index = ReadIndex(entryHolder),
                        Status = ReadState(entryHolder),
                        Term = entryTerm
                    };

                    if (entry.Status != HistoryStatus.Appended)
                        throw new InvalidOperationException($"Can't cancel the entry with index {entry.Index} and term {entry.Term}, it is already committed.");

                    toCancel.Add(entry);
                }

                foreach (var entry in toCancel)
                {
                    UpdateInternal(context, entry.Guid, entry.Type, entry.Index, term, entry.Status, null, new OperationCanceledException(msg));
                }
            }
        }

        public IEnumerable<DynamicJsonValue> GetHistoryLogs(TransactionOperationContext context)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(LogHistoryTable, LogHistorySlice);
            foreach (var entryHolder in table.SeekForwardFrom(LogHistoryTable.FixedSizeIndexes[LogHistoryDateTimeSlice], 0, 0))
            {
                yield return ReadHistoryLog(context, entryHolder);
            }
        }

        public unsafe List<DynamicJsonValue> GetLogByIndex(TransactionOperationContext context, long index)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(LogHistoryTable, LogHistorySlice);
            var reversedIndex = Bits.SwapBytes(index);
            using (Slice.External(context.Allocator, (byte*)&reversedIndex, sizeof(long), out var key))
            {
                var res = new List<DynamicJsonValue>();
                foreach (var entryHolder in table.SeekForwardFrom(LogHistoryTable.Indexes[LogHistoryIndexSlice], key, 0))
                {
                    var entry = ReadHistoryLog(context, entryHolder.Result);
                    if (entry[nameof(LogHistoryColumn.Index)].Equals(index) == false)
                    {
                        break;
                    }
                    res.Add(entry);
                }
                return res;
            }
        }

        private static unsafe DynamicJsonValue ReadHistoryLog(TransactionOperationContext context, Table.TableValueHolder entryHolder)
        {
            var djv = new DynamicJsonValue();

            var ticks = Bits.SwapBytes(*(long*)entryHolder.Reader.Read((int)(LogHistoryColumn.Ticks), out _));
            djv["Date"] = new DateTime(ticks);

            int size;
            djv[nameof(LogHistoryColumn.Guid)] = ReadGuid(entryHolder);
            djv[nameof(LogHistoryColumn.Index)] = ReadIndex(entryHolder);
            djv[nameof(LogHistoryColumn.Term)] = ReadTerm(entryHolder);
            djv[nameof(LogHistoryColumn.CommittedTerm)] = ReadCommittedTerm(entryHolder);
            djv[nameof(LogHistoryColumn.Type)] = ReadType(entryHolder);
            djv[nameof(LogHistoryColumn.State)] = ReadState(entryHolder).ToString();

            var resultPtr = entryHolder.Reader.Read((int)(LogHistoryColumn.Result), out size);
            if (size > 0)
            {
                var blittableResult = new BlittableJsonReaderObject(resultPtr, size, context);
                djv[nameof(LogHistoryColumn.Result)] = blittableResult.ToString();
                blittableResult.Dispose();
            }
            else
            {
                djv[nameof(LogHistoryColumn.Result)] = null;
            }

            var exTypePtr = entryHolder.Reader.Read((int)(LogHistoryColumn.ExceptionType), out size);
            djv[nameof(LogHistoryColumn.ExceptionType)] = size > 0 ? Encoding.UTF8.GetString(exTypePtr, size) : null;

            var exMsg = entryHolder.Reader.Read((int)(LogHistoryColumn.ExceptionMessage), out size);
            djv[nameof(LogHistoryColumn.ExceptionMessage)] = size > 0 ? Encoding.UTF8.GetString(exMsg, size) : null;

            return djv;
        }

        private static unsafe long ReadCommittedTerm(Table.TableValueHolder entryHolder)
        {
            return *(long*)entryHolder.Reader.Read((int)(LogHistoryColumn.CommittedTerm), out _);
        }

        private static unsafe HistoryStatus ReadState(Table.TableValueHolder entryHolder)
        {
            return (*(HistoryStatus*)entryHolder.Reader.Read((int)(LogHistoryColumn.State), out _));
        }

        private static unsafe string ReadType(Table.TableValueHolder entryHolder)
        {
            int size;
            var typeString = entryHolder.Reader.Read((int)(LogHistoryColumn.Type), out size);
            var type = Encoding.UTF8.GetString(typeString, size);
            return type;
        }

        private static unsafe long ReadTerm(Table.TableValueHolder entryHolder)
        {
            return *(long*)entryHolder.Reader.Read((int)(LogHistoryColumn.Term), out _);
        }

        private static unsafe long ReadIndex(Table.TableValueHolder entryHolder)
        {
            return Bits.SwapBytes(*(long*)entryHolder.Reader.Read((int)(LogHistoryColumn.Index), out _));
        }

        private static unsafe string ReadGuid(Table.TableValueHolder entryHolder)
        {
            var guidPtr = entryHolder.Reader.Read((int)(LogHistoryColumn.Guid), out var size);
            var guid = Encoding.UTF8.GetString(guidPtr, size);
            return guid;
        }

        public bool ContainsGuid(ClusterOperationContext context, string guid)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(LogHistoryTable, LogHistorySlice);
            if (table == null)
                return false;

            using (Slice.From(context.Allocator, guid, out var guidSlice))
            {
                return table.VerifyKeyExists(guidSlice);
            }
        }

        public unsafe bool HasHistoryLog(ClusterOperationContext context, BlittableJsonReaderObject cmd, out long index, out object result, out Exception exception)
        {
            result = null;
            exception = null;
            index = 0;

            var guid = GetGuidFromCommand(cmd);
            if (guid == null) // shouldn't happened in new cluster version!
                return false;

            if (guid == RaftIdGenerator.DontCareId)
                return false;

            var table = context.Transaction.InnerTransaction.OpenTable(LogHistoryTable, LogHistorySlice);
            using (Slice.From(context.Allocator, guid, out var guidSlice))
            {
                if (table.ReadByKey(guidSlice, out var reader) == false)
                    return false;

                index = Bits.SwapBytes(*(long*)reader.Read((int)LogHistoryColumn.Index, out _));
                var resultPtr = reader.Read((int)LogHistoryColumn.Result, out int size);
                if (size > 0)
                {
                    var blittableResultHolder = new BlittableJsonReaderObject(resultPtr, size, context);
                    if (blittableResultHolder.TryGet("Result", out result) == false) // this blittable will be clone to the outer context
                    {
                        throw new InvalidOperationException("Invalid result format!"); // shouldn't happened!
                    }
                }
                var exceptionTypePtr = reader.Read((int)LogHistoryColumn.ExceptionType, out size);
                if (size > 0)
                {
                    var exceptionMsgPtr = reader.Read((int)LogHistoryColumn.ExceptionType, out size);
                    var exceptionMsg = Encodings.Utf8.GetString(exceptionMsgPtr, size);

                    try
                    {
                        var exceptionType = Type.GetType(Encodings.Utf8.GetString(exceptionTypePtr, size));
                        exception = (Exception)Activator.CreateInstance(exceptionType, exceptionMsg);
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                        {
                            _log.Info($"failed to generate the exception dynamically for guid {guid}", e);
                        }

                        exception = new Exception($"failed to generate the exception dynamically, but the original exception message is:" +
                                                  $"{Environment.NewLine}{exceptionMsg}");
                    }
                }

                return true;
            }
        }
    }
}
