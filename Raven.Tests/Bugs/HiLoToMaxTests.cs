using System.Collections.Generic;
using Raven.Client.Document;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class HiLoToMaxTests : RavenTest
	{
		[Fact]
		public void FromScratch()
		{
			using(var store = NewDocumentStore())
			{
				var generator = new HiLoKeyGenerator("users", 3);
				for (long i = 0; i < 50; i++)
				{
					Assert.Equal(i + 1, generator.NextId(store.DatabaseCommands));
				}
			}
		}

		[Fact]
		public void ConcurrentWillNoGenerateConflicts()
		{
			using (var store = NewDocumentStore())
			{
				var generator1 = new HiLoKeyGenerator("users", 3);
				var generator2 = new HiLoKeyGenerator("users", 3);
				var dic = new Dictionary<long, int>();
				for (long i = 0; i < 50; i++)
				{
					dic.Add(generator1.NextId(store.DatabaseCommands), 1);
					dic.Add(generator2.NextId(store.DatabaseCommands), 1);
				}
			}
		}


		[Fact]
		public void CanUpgradeFromOldHiLo()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.Put("Raven/Hilo/users", null, 
					RavenJObject.FromObject(new{ ServerHi = 2}), 
					new RavenJObject());

				var generator = new HiLoKeyGenerator("users", 1024);
				for (long i = 0; i < 50; i++)
				{
					Assert.Equal((i + 1) + 1024, generator.NextId(store.DatabaseCommands));
				}
			}
		}
	}
}