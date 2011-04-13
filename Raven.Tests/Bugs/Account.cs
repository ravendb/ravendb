using System.Collections.Generic;

namespace Raven.Tests.Bugs
{
    public class Account
    {
        public Account()
        {
            Transactions = new List<Transaction>();
        }

        public IList<Transaction> Transactions { get; private set; }
    }
}