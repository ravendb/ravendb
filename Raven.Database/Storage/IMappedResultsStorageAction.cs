//-----------------------------------------------------------------------
// <copyright file="IMappedResultsStorageAction.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Storage
{
	public interface IMappedResultsStorageAction
	{
		void PutMappedResult(string view, string docId, string reduceKey, JObject data, byte[] viewAndReduceKeyHashed);
		IEnumerable<JObject> GetMappedResults(string view, string reduceKey, byte[] viewAndReduceKeyHashed);
		IEnumerable<string> DeleteMappedResultsForDocumentId(string documentId, string view);
		void DeleteMappedResultsForView(string view);
	}
}
