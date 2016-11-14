// -----------------------------------------------------------------------
//  <copyright file="CounterChanges.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

namespace Raven.NewClient.Abstractions.Counters
{
    public static class CounterUtils
    {
        public static string GetFullCounterName(string groupName, string counterName)
        {
            return string.Concat(groupName, "/", counterName);
        }
    }
}
