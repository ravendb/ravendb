using System;

namespace Raven.Database
{
    public class TransactionInformation
    {
        public Guid Id { get; set; }
        public TimeSpan Timeout { get; set; }
    }
}