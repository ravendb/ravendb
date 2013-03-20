using System.Linq;
using Raven.Client.Indexes;

namespace Raven.Tests.Bugs.TransformResults
{
	public class ThorIndex : AbstractIndexCreationTask<Thor>
	{
		public ThorIndex()
		{
			Map = thors => from doc in thors
						   select new {doc.Name};
			TransformResults = (database, thors) =>
				from item in thors
				select new {Id = item.Id, item.Name};
		}
	}
}