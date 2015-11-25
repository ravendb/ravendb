// -----------------------------------------------------------------------
//  <copyright file="TransactionState.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;

using Raven.Abstractions.Extensions;
using Raven.Database.Storage;

namespace Raven.Database.Impl.DTC
{
    public class TransactionState
    {
        public readonly List<DocumentInTransactionData> Changes = new List<DocumentInTransactionData>();

        [CLSCompliant(false)]
        public volatile Reference<DateTime> LastSeen = new Reference<DateTime>();

        private TimeSpan timeout;
        public TimeSpan Timeout
        {
            get
            {
                if (timeout < TimeSpan.FromSeconds(30))
                    return TimeSpan.FromSeconds(30);
                return timeout;
            }

            set { timeout = value; }
        }

        public TransactionState()
        {
            Timeout = TimeSpan.FromMinutes(3);
        }
    }
}