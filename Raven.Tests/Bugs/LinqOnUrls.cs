//-----------------------------------------------------------------------
// <copyright file="LinqOnUrls.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class LinqOnUrls : RavenTest
	{
		[Fact]
		public void CanQueryUrlsValuesUsingLinq()
		{
			var port = 8079;
			using (GetNewServer(port))
			{
				using (var store = new DocumentStore { Url = "http://localhost:" + port }.Initialize())
				using (var session = store.OpenSession())
				{
// ReSharper disable once ReturnValueOfPureMethodIsNotUsed
					session.Query<User>().FirstOrDefault(x => x.Name == "http://www.idontexistinthecacheatall.com?test=xxx&gotcha=1");
				}
			}
		}
	}
}