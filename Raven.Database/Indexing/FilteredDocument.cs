// -----------------------------------------------------------------------
//  <copyright file="FilteredDocument.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;

namespace Raven.Database.Indexing
{
	public class FilteredDocument 
	{
		public FilteredDocument(IJsonDocumentMetadata doc)
		{
			__document_id = doc.Key;
		}

		[CLSCompliant(false)]
		public string __document_id { get; set; }
	}
}