// -----------------------------------------------------------------------
//  <copyright file="ReplicatedEtagInfo.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

using Raven.Abstractions.Data;

namespace Raven.Abstractions.Replication
{
    public class ReplicatedEtagInfo
    {
        public string DestinationUrl { get; set; }
        public long? DocumentEtag { get; set; }

        public override string ToString()
        {
            return string.Format("Url: {0}, Etag: {1}", DestinationUrl, DocumentEtag == null ? "no etag" : DocumentEtag.ToString());
        }
    }
}
