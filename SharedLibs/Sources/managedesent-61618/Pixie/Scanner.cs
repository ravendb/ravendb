//-----------------------------------------------------------------------
// <copyright file="Scanner.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Sql.Parsing
{
    /// <summary>
    /// Scanner implementation.
    /// </summary>
    internal partial class Scanner : ScanBase
    {
        /// <summary>
        /// Set the string to be tokenized by the scanner.
        /// </summary>
        /// <param name="command">The string to be tokenized.</param>
        public void SetSource(string command)
        {
            this.SetSource(command, 0);
        }
    }
}
