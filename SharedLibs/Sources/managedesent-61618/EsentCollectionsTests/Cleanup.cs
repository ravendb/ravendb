// --------------------------------------------------------------------------------------------------------------------
// <copyright file="Cleanup.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   Methods for test cleanup.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace EsentCollectionsTests
{
    using System;
    using System.IO;
    using System.Threading;

    /// <summary>
    /// Methods for test cleanup.
    /// </summary>
    public static class Cleanup
    {
        /// <summary>
        /// Delete a directory, retrying the operation if the delete fails.
        /// </summary>
        /// <param name="directory">
        /// The directory to delete.
        /// </param>
        public static void DeleteDirectoryWithRetry(string directory)
        {
            const int MaxAttempts = 3;
            for (int attempt = 1; attempt <= MaxAttempts; ++attempt)
            {
                try
                {
                    if (Directory.Exists(directory))
                    {
                        Directory.Delete(directory, true);
                    }

                    return;
                }
                catch (UnauthorizedAccessException)
                {
                    if (attempt == MaxAttempts)
                    {
                        throw;
                    }
                }
                catch (IOException)
                {
                    if (attempt == MaxAttempts)
                    {
                        throw;
                    }
                }

                Thread.Sleep(TimeSpan.FromSeconds(1));
            }
        }
    }
}