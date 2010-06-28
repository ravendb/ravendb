using System.Collections.Generic;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;
using Raven.Database.Storage.StorageActions;

namespace Raven.Storage.Managed.StroageActions
{
	public class MappedResultsStorageAction : AbstractStorageActions, IMappedResultsStorageAction
	{
		public void PutMappedResult(string view, string docId, string reduceKey, JObject data, byte[] viewAndReduceKeyHashed)
		{
			var pos = Writer.Position;
			BinaryWriter.Write(reduceKey);
			data.WriteTo(new BsonWriter(Writer));
			Mutator.MappedResultsByDocumentId.GetOrCreateBag(
				GetViewAndDocumentId(view, docId))
				.Add(pos);
			
			Mutator.MappedResultsByReduceKey
				.GetOrCreateBag(GetViewAndReduceKey(reduceKey, view))
				.Add(pos);
		}

		private static JObject GetViewAndDocumentId(string view, string docId)
		{
			return new JObject(
				new JProperty("View", view),
				new JProperty("DocID", docId)
				);
		}

		private static JObject GetViewAndReduceKey(string reduceKey, string view)
		{
			return new JObject(
				new JProperty("View", view),
				new JProperty("ReduceKey", reduceKey)
				);
		}

		public IEnumerable<JObject> GetMappedResults(string view, string reduceKey, byte[] viewAndReduceKeyHashed)
		{
			var bag = Viewer.MappedResultsByReduceKey
				.GetBag(GetViewAndReduceKey(reduceKey, view));
			if(bag == null)
				yield break;
			foreach (var result in bag)
			{
				Reader.Position = result;
				BinaryReader.ReadString();// skipping the reduce key
				yield return JObject.Load(new BsonReader(Reader));
			}
		}

		public IEnumerable<string> DeleteMappedResultsForDocumentId(string documentId, string view)
		{
			var bag = Mutator.MappedResultsByDocumentId
				.GetBag(GetViewAndDocumentId(view, documentId));
			if(bag == null)
				yield break;

			foreach (var pos in bag)
			{
				Reader.Position = pos;
				var reduceKey = BinaryReader.ReadString();

				var reduceKeyBag = Mutator.MappedResultsByReduceKey.GetBag(GetViewAndReduceKey(reduceKey, view));
				if (reduceKeyBag != null)
					reduceKeyBag.Remove(pos);

				yield return reduceKey;
			}
		}

		public void DeleteMappedResultsForView(string view)
		{
			var viewKey = new JObject(new JProperty("View", view));
			Mutator.MappedResultsByDocumentId.DeleteAllMatching(viewKey);
			Mutator.MappedResultsByReduceKey.DeleteAllMatching(viewKey);
		}
	}
}