using System.Linq;

using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;

namespace Raven.Tests.MailingList.Thor
{
	public class TransportsIndex : AbstractMultiMapIndexCreationTask<JoinedChildTransport>
	{
		public TransportsIndex()
		{
			AddMap<Child>(childList => from child in childList
			                           select new
			                           {
			                           	ChildId = child.Id,
			                           	TransportId = (string)null,
			                           	Name = child.Name,
			                           });

			AddMap<Transport>(transportList => from transport in transportList
			                                   select new
			                                   {
			                                   	ChildId = transport.ChildId,
			                                   	TransportId = transport.Id,
			                                   	Name = (string)null,
			                                   });

			Reduce = results => from result in results
			                    group result by result.ChildId
			                    into g
			                    from transport in g.Where(transport => transport.TransportId != null).DefaultIfEmpty()
			                    from child in g.Where(barn => barn.Name != null).DefaultIfEmpty()
			                    select new { ChildId = g.Key, transport.TransportId, child.Name };


			Store(x => x.ChildId, FieldStorage.Yes);
			Store(x => x.TransportId, FieldStorage.Yes);
			Store(x => x.Name, FieldStorage.Yes);
		}
	}
}