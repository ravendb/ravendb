//-----------------------------------------------------------------------
// <copyright file="ijet_logtime.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;

    /// <summary>
    /// Interface for common methods between JET_LOGTIME and JET_BKLOGTIME.
    /// </summary>
    public interface IJET_LOGTIME : INullableJetStruct
    {
        /// <summary>
        /// Generate a DateTime representation of this IJET_LOGTIME.
        /// </summary>
        /// <returns>
        /// A DateTime representing the IJET_LOGTIME. If the IJET_LOGTIME
        /// is null then null is returned.
        /// </returns>
        DateTime? ToDateTime();
    }
}