// -----------------------------------------------------------------------
//  <copyright file="IOperationState.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Raven.Json.Linq;

namespace Raven.Abstractions.Data
{
    public interface IOperationState
    {
        bool Completed { get; }
        bool Faulted { get; }
        bool Canceled { get; }
        string State { get; }
        Exception Exception { get; }
    }
}
