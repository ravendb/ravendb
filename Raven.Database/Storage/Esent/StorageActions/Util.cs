//-----------------------------------------------------------------------
// <copyright file="Util.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Database.Extensions;

namespace Raven.Storage.Esent.StorageActions
{
	public partial class DocumentStorageActions
	{

		private Guid EnsureDocumentEtagMatch(string key, Guid? etag, string method)
		{
			var existingEtag = Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"]).TransfromToGuidWithProperSorting();
			if (existingEtag != etag && etag != null)
			{
				if(etag == Guid.Empty)
				{
					var metadata = Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["metadata"]).ToJObject();
					if(metadata.ContainsKey(Constants.RavenDeleteMarker) && 
						metadata.Value<bool>(Constants.RavenDeleteMarker))
					{
						return existingEtag;
					}
				}

				throw new ConcurrencyException(method + " attempted on document '" + key +
											   "' using a non current etag")
				{
					ActualETag = existingEtag,
					ExpectedETag = etag.Value
				};
			}
			return existingEtag;
		}
	}
}
