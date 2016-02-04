// -----------------------------------------------------------------------
//  <copyright file="CounterSmugglerOperationState.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;

namespace Raven.Abstractions.Database.Smuggler.Counter
{
    public class CounterSmugglerOperationState
    {
        public Etag LastEtag { get; set; }
    }
}