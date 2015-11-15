//-----------------------------------------------------------------------
// <copyright file="TransactionInformation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

namespace Raven.Abstractions.Data
{
    /// <summary>
    /// Transaction information that identify the transaction id and timeout
    /// </summary>
    public class TransactionInformation
    {
        /// <summary>
        /// Transaction identifier.
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// Transaction timeout.
        /// </summary>
        public TimeSpan Timeout { get; set; }
    }
}
