// -----------------------------------------------------------------------
//  <copyright file="Utils.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Runtime.CompilerServices;

namespace Raven.Server.Json
{
    public static class Utils
    {
        //TODO: replace with Voron.Utils.NearestPowerOfTwo when voron will accessible
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long GetNextPowerOfTwo(long v) {
            v--;
            v |= v >> 1;
            v |= v >> 2;
            v |= v >> 4;
            v |= v >> 8;
            v |= v >> 16;
            v++;
            return v;
        }
}
}