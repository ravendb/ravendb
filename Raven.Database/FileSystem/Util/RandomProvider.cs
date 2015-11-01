// -----------------------------------------------------------------------
//  <copyright file="RandomProvider.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.FileSystem.Util
{
    using System;
    using System.Threading;

    public static class RandomProvider
    {
        private static int seed = Environment.TickCount;

        private static readonly ThreadLocal<Random> RandomWrapper = new ThreadLocal<Random>(() =>
            new Random(Interlocked.Increment(ref seed))
        );

        public static Random GetThreadRandom()
        {
            return RandomWrapper.Value;
        }
    }
}
