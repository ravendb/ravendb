﻿using System;
using Raven.Client.Documents.Session.Querying.Sharding;

namespace Raven.Client.Documents.Session;

public partial class AsyncDocumentQuery<T>
{
    IAsyncDocumentQuery<T> IAsyncDocumentQuery<T>.ShardContext(Action<IShardedQueryContextBuilder> builder)
    {
        ShardContext(builder);
        return this;
    }
}
