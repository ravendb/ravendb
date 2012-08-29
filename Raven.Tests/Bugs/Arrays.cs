using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class Arrays : RavenTest
	{
		[Fact]
		public void CanRetrieveMultiDimensionalArray()
		{
			using (var store = NewDocumentStore())
			{
				var arrayValue = new double[,] 
				{
					{ 1, 2 }, 
					{ 3, 4 }, 
					{ 5, 6 } 
				};

				using (var session = store.OpenSession())
				{
					session.Store(new ArrayHolder
						{
							ArrayValue = arrayValue
						});

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var arrayHolder = session.Load<ArrayHolder>("arrayholders/1");

					Assert.Equal(arrayValue, arrayHolder.ArrayValue);
				}
			}
		}

		public class ArrayHolder
		{
			public double[,] ArrayValue { get; set; }
		}
	}
}
