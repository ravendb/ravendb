using System.Collections.Concurrent;
using System.Threading.Tasks;
using Raven.Abstractions.Data;

namespace Raven.Client.Changes
{
	public interface IDatabaseChanges
	{
		Task Task { get; }
		IObservableWithTask<IndexChangeNotification> IndexSubscription(string indexName);
		IObservableWithTask<DocumentChangeNotification> DocumentSubscription(string docId);
		IObservableWithTask<DocumentChangeNotification> DocumentPrefixSubscription(string docIdPrefix);
	}
}