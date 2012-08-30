//-----------------------------------------------------------------------
// <copyright file="WhenRavenClrTypeNotFound.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Client.Document;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class WhenRavenClrTypeNotFound : RavenTest
	{
		[Fact]
		public void WillStillBeAbleToDeserialize()
		{
			using(var store = NewDocumentStore())
			{
				using(var s = store.OpenSession())
				{
					s.Store(new User());
					s.SaveChanges();
				}

				var jsonDocument = store.DatabaseCommands.Get("users/1");
				jsonDocument.Metadata["Raven-Clr-Type"] =
					jsonDocument.Metadata.Value<string>("Raven-Clr-Type").Replace("Raven", "Rhino");

				store.DatabaseCommands.Put("users/1", null, jsonDocument.DataAsJson, jsonDocument.Metadata);

				using (var s = store.OpenSession())
				{
					Assert.DoesNotThrow(() => s.Load<User>("users/1"));
				}
			}
		}
	}
}
