// -----------------------------------------------------------------------
//  <copyright file="StopwatchScope.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;

namespace Raven.Database.Util
{
    internal class StopwatchScope : IDisposable
    {
        private readonly Stopwatch sw;

        private StopwatchScope(Stopwatch sw)
        {
            this.sw = sw;
            sw.Start();
        }

        public void Dispose()
        {
            sw.Stop();
        }

        public static StopwatchScope For(Stopwatch sw, bool resetBeforeStart = false)
        {
            if (sw == null)
                return null;

            if(resetBeforeStart)
                sw.Reset();

            return new StopwatchScope(sw);
        }
    }
}
