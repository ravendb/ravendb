// -----------------------------------------------------------------------
//  <copyright file="ChangedDoc.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;

namespace Raven.Database.Impl.DTC
{
    public class ChangedDoc
    {
        public string transactionId;
        public Etag currentEtag;
        public Etag committedEtag;
    }
}