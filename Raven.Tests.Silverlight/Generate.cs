namespace Raven.Tests.Silverlight
{
	using System.Collections.Generic;
	using System.Linq;
	using System.Threading.Tasks;
	using Client.Document;
	using Document;
	using Entities;
	using Microsoft.Silverlight.Testing;
	using Microsoft.VisualStudio.TestTools.UnitTesting;

	/// <summary>
	/// Not actually a test, just an easy way for me to insert some sample data
	/// </summary>
	public class Generate : RavenTestBase
	{
		[Asynchronous][Ignore]
		public IEnumerable<Task> Some_sample_data()
		{
			var store = new DocumentStore {Url = Url + Port};
			store.Initialize();

			using (var session = store.OpenAsyncSession())
			{
				Enumerable.Range(0, 25).ToList()
					.ForEach(i => session.Store(new Company {Id = "Companies/" + i, Name = i.ToString()}));

				Enumerable.Range(0, 25).ToList()
					.ForEach(i => session.Store(new Order {Id = "Orders/" + i, Note = i.ToString()}));

				yield return session.SaveChangesAsync();
			}
		}
	}
}