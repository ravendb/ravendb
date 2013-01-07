//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Utilities
{
    using System;

    /// <summary>
    /// Contains the static method that starts the program.
    /// </summary>
    public class Program
    {
        /// <summary>
        /// Main method, called when the program starts.
        /// </summary>
        /// <param name="args">Arguments to the program.</param>
        /// <returns>0 for success, non-zero for a failure.</returns>
        public static int Main(string[] args)
        {
            var dbutil = new Dbutil();
            try
            {
                dbutil.Execute(args);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine("Caught exception:");
                Console.Error.WriteLine(ex.ToString());
                return 1;
            }

            return 0;
        }
    }
}
