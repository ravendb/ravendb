// -----------------------------------------------------------------------
//  <copyright file="CounterChanges.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

namespace Raven.Abstractions.Counters
{
    public class CounterChanges
    {
        public string FullCounterName { get; set; }
        public long Delta { get; set; }
    }
}
