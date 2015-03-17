//-----------------------------------------------------------------------
// <copyright file="Util.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;

namespace Raven.Database.Storage.Esent.StorageActions
{
	public partial class DocumentStorageActions
	{
		private Etag EnsureDocumentEtagMatch(string key, Etag etag, string method)
		{
			var existingEtag = Etag.Parse(Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["etag"]));
			if (etag != null)
			{
				Etag next;
				while (etagTouches.TryGetValue(etag, out next))
				{
					etag = next;
				}
				if (existingEtag != etag)
				{
					if (etag == Etag.Empty)
					{
						var metadata = Api.RetrieveColumn(session, Documents, tableColumnsCache.DocumentsColumns["metadata"]).ToJObject();
						if (metadata.ContainsKey(Constants.RavenDeleteMarker) &&
							metadata.Value<bool>(Constants.RavenDeleteMarker))
						{
							return existingEtag;
						}
					}

					throw new ConcurrencyException(method + " attempted on document '" + key +
												   "' using a non current etag")
					{
						ActualETag = existingEtag,
						ExpectedETag = etag
					};
				}
			}
			return existingEtag;
		}

	}
}
