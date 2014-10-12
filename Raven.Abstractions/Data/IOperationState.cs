// -----------------------------------------------------------------------
//  <copyright file="IOperationState.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Json.Linq;

namespace Raven.Abstractions.Data
{
    public interface IOperationState
    {
        bool Completed { get; }
        bool Faulted { get; }
        RavenJToken State { get; }
    }
}