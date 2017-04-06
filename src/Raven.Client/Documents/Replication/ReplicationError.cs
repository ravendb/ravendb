//-----------------------------------------------------------------------
// <copyright file="ServerError.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Raven.Client.Documents.Replication
{
    public class ReplicationError
    {
        public string Error { get; set; }
        public DateTime Timestamp { get; set; }

        public override string ToString()
        {
            return $"Error: {Error}";
        }
    }
}
