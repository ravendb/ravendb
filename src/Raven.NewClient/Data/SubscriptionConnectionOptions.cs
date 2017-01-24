// -----------------------------------------------------------------------
//  <copyright file="SubscriptionBatchOptions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net;
using System.Threading;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Client.Extensions;
using Newtonsoft.Json;
using Sparrow.Json;


namespace Raven.NewClient.Abstractions.Data
{
    public class SubscriptionConnectionClientMessage
    {
        public enum MessageType
        {
            None,
            Acknowledge
        }

        public MessageType Type { get; set; }
        public long Etag { get; set; }
    }

    public class SubscriptionConnectionServerMessage : IDisposable
    {
        internal BlittableJsonReaderObject ParentObjectToDispose;

        public enum MessageType
        {
            None,
            CoonectionStatus,
            EndOfBatch,
            Data,
            Confirm,
            Error
        }

        public enum ConnectionStatus
        {
            None,
            Accepted,
            InUse,
            Closed,
            NotFound
        }

        public MessageType Type { get; set; }
        public ConnectionStatus Status { get; set; }
        public BlittableJsonReaderObject Data { get; set; }
        public string Exception { get; set; }

        public void Dispose()
        {
            ParentObjectToDispose?.Dispose();
            ParentObjectToDispose = null;
        }
    }
  
    public class SubscriptionConnectionOptions
    {
        public long SubscriptionId { get; set; }

        public SubscriptionConnectionOptions()
        {
            Strategy = SubscriptionOpeningStrategy.OpenIfFree;
            MaxDocsPerBatch = 4096;
        }

        public int TimeToWaitBeforeConnectionRetryMilliseconds { get; set; } = 5000;
        public bool IgnoreSubscribersErrors { get; set; }
        public SubscriptionOpeningStrategy Strategy { get; set; }
        public int MaxDocsPerBatch { get; set; }
    }        
}