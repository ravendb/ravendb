//-----------------------------------------------------------------------
// <copyright file="DocumentInTransactionData.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Storage
{
    public class DocumentInTransactionData
    {
        public Guid Etag { get; set; }
        public bool Delete { get; set; }
        public JObject Metadata { get; set; }
		public JObject Data { get; set; }
        public string Key { get; set; }

    }
}
