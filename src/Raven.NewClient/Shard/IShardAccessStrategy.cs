//-----------------------------------------------------------------------
// <copyright file="IShardAccessStrategy.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Connection.Async;

namespace Raven.NewClient.Client.Shard
{
    /// <summary>
    /// Apply an operation to all the shard session
    /// </summary>
    public interface IShardAccessStrategy
    {
    }
}
