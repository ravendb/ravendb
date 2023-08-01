using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Rachis.Commands
{
    public sealed class RemoveEntryFromRaftLogCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
    {
        private readonly long _index;
        private readonly string _tag;
        private readonly RachisLogHistory _logHistory;
        public bool Succeeded { get; private set; }

        public RemoveEntryFromRaftLogCommand(string tag, long index, RachisLogHistory logHistory)
        {
            _index = index;
            _tag = tag;
            _logHistory = logHistory;
        }

        protected override long ExecuteCmd(ClusterOperationContext context)
        {
            Succeeded = RemoveEntryFromRaftLogInTx(context, _index);
            return 1;
        }

        private unsafe bool RemoveEntryFromRaftLogInTx(ClusterOperationContext context, long index)
        {
            Table table = context.Transaction.InnerTransaction.OpenTable(RachisConsensus.LogsTable, RachisConsensus.EntriesSlice);
            long reversedIndex = Bits.SwapBytes(index);

            long id;
            long term;
            using (Slice.External(context.Allocator, (byte*)&reversedIndex, sizeof(long), out Slice key))
            {
                if (table.ReadByKey(key, out TableValueReader reader))
                {
                    term = *(long*)reader.Read(1, out int size);
                    id = reader.Id;
                }
                else
                {
                    return false;
                }
            }

            var noopCmd = new DynamicJsonValue
            {
                ["Type"] = $"Noop for {_tag} in term {term}",
                ["Command"] = "noop"
            };
            var cmd = context.ReadObject(noopCmd, "noop-cmd");

            using (table.Allocate(out TableValueBuilder tvb))
            {
                tvb.Add(reversedIndex);
                tvb.Add(term);
                tvb.Add(cmd.BasePointer, cmd.Size);
                tvb.Add((int)RachisEntryFlags.Noop);
                table.Update(id, tvb, true);
            }

            _logHistory.UpdateHistoryLogPreservingGuidAndStatus(context, index, term, cmd);

            return true;
        }

        public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
        {
            throw new NotImplementedException();
        }
    }

}
