using System;
using System.Collections.Generic;
using Raven.Server.Documents.Replication;

namespace Raven.Server
{
    public static class ChangeVectorExtensions
    {
		public static bool UpdateLargerEtagIfRelevant(this ChangeVectorEntry[] changeVector,
				   Dictionary<Guid, long> maxEtagsPerDbId)
		{
			var changeVectorUpdated = false;
			for (int i = 0; i < changeVector.Length; i++)
			{
				long dbEtag;
				if (maxEtagsPerDbId.TryGetValue(changeVector[i].DbId, out dbEtag) == false)
					continue;
				maxEtagsPerDbId.Remove(changeVector[i].DbId);
				if (dbEtag > changeVector[i].Etag)
				{
					changeVectorUpdated = true;
					changeVector[i].Etag = dbEtag;
				}
			}

			return changeVectorUpdated;
		}

		public static ChangeVectorEntry[] InsertNewEtagsIfRelevant(
			this ChangeVectorEntry[] changeVector,
			Dictionary<Guid, long> maxEtagsPerDbId, 
			out bool hasResized)
		{
			hasResized = false;

			if (maxEtagsPerDbId.Count <= 0)
				return changeVector;

			hasResized = true;
			var oldSize = changeVector.Length;
			Array.Resize(ref changeVector, oldSize + maxEtagsPerDbId.Count);

			foreach (var kvp in maxEtagsPerDbId)
			{
				changeVector[oldSize++] = new ChangeVectorEntry
				{
					DbId = kvp.Key,
					Etag = kvp.Value,
				};
			}

			return changeVector;
		}
	}
}
