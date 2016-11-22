// -----------------------------------------------------------------------
//  <copyright file="AsyncOperationResult.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.NewClient.Client.Connection.Request
{
    public class AsyncOperationResult<T>
    {
        public T Result;
        public bool WasTimeout;
        public bool Success;
        public Exception Error;
    }
}
