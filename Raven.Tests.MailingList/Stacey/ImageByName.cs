using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Common.Analyzers;

namespace Raven.Tests.MailingList.Stacey
{
	public class ImageByName : AbstractIndexCreationTask<Image, ImageByName.ReduceResult>
	{

		public class ReduceResult
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		public ImageByName()
		{
			Map = docs => from i in docs
			              select new
			              {
			              	Id = i.Id,
			              	Name = new[] { i.Name },
			              };
			Index(r => r.Name, FieldIndexing.Analyzed);
			Analyzers.Add(n => n.Name, typeof(NGramAnalyzer).AssemblyQualifiedName);
		}
	}
}