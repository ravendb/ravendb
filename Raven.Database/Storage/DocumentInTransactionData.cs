//-----------------------------------------------------------------------
// <copyright file="DocumentInTransactionData.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Storage
{
	public class DocumentInTransactionData
	{
		public Etag Etag { get; set; }

		public Etag CommittedEtag { get; set; }

		public bool Delete { get; set; }
		public RavenJObject Metadata { get; set; }
		public RavenJObject Data { get; set; }
		public string Key { get; set; }
		public DateTime LastModified { get; set; }

	}
}
