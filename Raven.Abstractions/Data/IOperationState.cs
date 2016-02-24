// -----------------------------------------------------------------------
//  <copyright file="IOperationState.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Json.Linq;

namespace Raven.Abstractions.Data
{
    public interface IOperationState
    {
        bool Completed { get; }
        bool Faulted { get; }
        bool Canceled { get; }
        Exception Exception { get; }

        /// <summary>
        /// Put progress information under Progress field
        /// and error in Error field
        /// </summary>
        RavenJObject State { get; }
    }
}
