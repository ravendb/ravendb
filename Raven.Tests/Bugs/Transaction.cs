using System;

namespace Raven.Tests.Bugs
{
    public class Transaction
    {
        public Transaction(int amount, DateTime date)
        {
            Amount = amount;
            Date = date;
        }

        public Transaction()
        {
           
        }

        public int Amount { get; private set; }
        public DateTime Date { get; private set; }
    }
}