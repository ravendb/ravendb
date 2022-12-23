using System;
using System.Runtime.InteropServices;
using System.Security.Cryptography;

namespace SlowTests.SparrowTests.VxSort
{
    /// <summary>
    /// Tests + Setup code comparing various quicksort to arraysort in terms of correctness/parity
    /// </summary>
    public class DataGeneration
    {
        internal static (int[] randomData, int[] sortedData) GenerateData(
            int size,
            int seed,
            int forcedValue = 0,
            double forcedValueRate = double.NaN,
            int modulo = int.MaxValue,
            bool dontSort = false
        )
        {
            var r = new Random(seed);
            var data = new int[size];
            for (var i = 0; i < size; ++i)
            {
                var rndValue = r.Next();

                data[i] = double.IsNaN(forcedValueRate)
                    ? rndValue % modulo
                    : r.NextDouble() > forcedValueRate
                        ? forcedValue
                        : (rndValue % modulo);
            }

            int[] sorted = null;
            if (!dontSort)
            {
                sorted = new int[size];
                data.CopyTo(sorted, 0);
                Array.Sort(sorted);
            }

            var reproContext = "";
            using (var sha1 = SHA1.Create())
            {
                Span<byte> hash = stackalloc byte[20];
                sha1.TryComputeHash(
                    MemoryMarshal.Cast<int, byte>(new ReadOnlySpan<int>(data)),
                    hash,
                    out _
                );
                var dataHash = Convert.ToBase64String(hash);
                sha1.TryComputeHash(
                    MemoryMarshal.Cast<int, byte>(new ReadOnlySpan<int>(sorted)),
                    hash,
                    out _
                );
                var sortedHash = Convert.ToBase64String(hash);

                reproContext = $"[{size},{seed}] -> [{dataHash},{sortedHash}]";
            }

            return (data, sorted);
        }
    }
}
