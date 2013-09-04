// --------------------------------------------------------------------------------------------------------------------
// <copyright file="ArrayShuffle.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   Contains an extension method to randomize an array.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace EsentCollectionsTests
{
    using System;

    /// <summary>
    /// Contains an extension method to randomize an array.
    /// </summary>
    public static class ArrayShuffle
    {
        /// <summary>
        /// Randomly shuffle an array.
        /// </summary>
        /// <typeparam name="T">The type of the array.</typeparam>
        /// <param name="data">The array to shuffle.</param>
        public static void Shuffle<T>(this T[] data)
        {
            var rand = new Random();
            for (int i = 0; i < data.Length; ++i)
            {
                int swapWith = rand.Next(i, data.Length);
                T temp = data[i];
                data[i] = data[swapWith];
                data[swapWith] = temp;
            }
        }        
    }
}