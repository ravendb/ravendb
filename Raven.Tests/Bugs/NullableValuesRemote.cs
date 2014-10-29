// //-----------------------------------------------------------------------
// // <copyright company="Hibernating Rhinos LTD">
// //     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// // </copyright>
// //-----------------------------------------------------------------------
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class NullableValuesRemote : RavenTest
	{
		[Fact]
		public void CanGetNullableDoubleAndInt()
		{
			using (GetNewServer())
			using (var documentStore = new DocumentStore {Url = "http://localhost:8079/"}.Initialize())
			{
				using(var s = documentStore.OpenSession())
				{
					s.Store(new WithNullables{D = null, I = null});
					s.SaveChanges();
				}

				using(var s = documentStore.OpenSession())
				{
					s.Query<WithNullables>().ToList();
				}
			}
		}

		#region Nested type: WithNullables

		public class WithNullables
		{
			public double? D { get; set; }
			public int? I { get; set; }
		}

		#endregion
	}
}