using Lucene.Net.Store;
using Voron.Impl;

namespace Raven.Server.Indexing
{
    public class VoronState : IState
    {
        public VoronState(Transaction transaction)
        {
            Transaction = transaction;
        }

        public readonly Transaction Transaction;
    }
}