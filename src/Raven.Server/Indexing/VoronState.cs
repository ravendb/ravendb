using Lucene.Net.Store;
using Voron.Impl;

namespace Raven.Server.Indexing
{
    public sealed class VoronState : IState
    {
        public VoronState(Transaction transaction)
        {
            Transaction = transaction;
        }

        public Transaction Transaction;
    }
}
