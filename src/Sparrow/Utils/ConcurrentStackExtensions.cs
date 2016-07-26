using System;
using System.Collections.Concurrent;
using System.Threading;

namespace Sparrow.Utils
{
    public static class ConcurrentStackExtensions
    {
        public static void ReduceSizeIfTooBig<T>(this ConcurrentStack<T> local, int maxSize)
            where T : IDisposable
        {
            if (local.Count < maxSize)
                return;

            ReduceSizeSafely(local);
        }

        private static void ReduceSizeSafely<T>(ConcurrentStack<T> local) where T : IDisposable
        {
            // we allow very deep buffers because there are many async requests per thread
            bool lockTaken = false;
            try
            {
                Monitor.TryEnter(local, 0, ref lockTaken);
                var temp = new T[local.Count/2];
                var count = local.TryPopRange(temp);
                local.PushRange(temp, 0, count/2);
                for (int i = count/2; i < temp.Length; i++)
                {
                    temp[i].Dispose();
                }
            }
            finally
            {
                if (lockTaken)
                    Monitor.Exit(local);
            }
        }
    }
}