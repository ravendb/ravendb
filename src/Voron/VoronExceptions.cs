using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Voron.Impl;

namespace Voron
{
    public static class VoronExceptions
    {
        public static void ThrowIfReadOnly(LowLevelTransaction tx, string message = "A write transaction is required for this operation")
        {
            if (tx.Flags == TransactionFlags.Read)
                throw new InvalidOperationException(message);
        }

        public static void ThrowIfReadOnly(Transaction tx, string message = "A write transaction is required for this operation")
        {
            if (tx.LowLevelTransaction.Flags == TransactionFlags.Read)
                throw new InvalidOperationException(message);
        }

        public static void ThrowIfNull(Slice argument, [CallerArgumentExpression(nameof(argument))] string paramName = null)
        {
            if (argument.HasValue == false)
            {
                throw new ArgumentNullException(paramName);
            }
        }
    }
}
