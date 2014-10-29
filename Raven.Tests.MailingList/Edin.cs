using System;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class Edin : RavenTest
	{
		public abstract class Parameter
		{
			public String Name { get; set; }
		}

		public class IntArrayParameter : Parameter
		{
			public Int32[,] Value { get; set; }
		}

		public class DoubleArrayParameter : Parameter
		{
			public double[,] Value { get; set; }
		}

		[Fact]
		public void CanUseMultiDimensionalArray_Int()
		{
			using (var store = NewDocumentStore())
			{
				Int32[,] ia = { { 11, 12 }, { 21, 22 } };
				IntArrayParameter iap = new IntArrayParameter { Name = "iap", Value = ia };


				using (var s = store.OpenSession())
				{
					s.Store(iap);
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var intArrayParameter = s.Load<IntArrayParameter>(1);
					Assert.Equal(iap.Value, intArrayParameter.Value);
				}
			}
		}

		[Fact]
		public void CanUseMultiDimensionalArray_Double()
		{
			using (var store = NewDocumentStore())
			{
				double[,] ia = { { 11, 12 }, { 21, 22 } };
				DoubleArrayParameter iap = new DoubleArrayParameter { Name = "iap", Value = ia };


				using (var s = store.OpenSession())
				{
					s.Store(iap);
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var intArrayParameter = s.Load<DoubleArrayParameter>(1);
					Assert.Equal(iap.Value, intArrayParameter.Value);
				}
			}
		}
	}
}