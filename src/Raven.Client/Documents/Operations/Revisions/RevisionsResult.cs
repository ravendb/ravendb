//-----------------------------------------------------------------------
// <copyright file="RevisionsConfiguration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;

namespace Raven.Client.Documents.Operations.Revisions
{
    public class RevisionsResult<T>
    {
        public List<T> Results { get; set; }

        public int TotalResults { get; set; }
    }
}
