// -----------------------------------------------------------------------
//  <copyright file="Chirea.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class Chirea : RavenTest
	{
		 public class Item
		 {
			 public string Name { get; set; }
		 }

		 public class Container
		 {
			 public Item First { get; set; }
			 public Item  Second { get; set; }
		 }

		 public class ContainsIndex : AbstractIndexCreationTask<Container>
		 {
			 public ContainsIndex()
			 {
				 Map = containers =>
				       from container in containers
				       from item in new[] {container.First, container.Second}
				       select new
				       {
					       item.Name
				       };
			 }
		 }

		 [Fact]
		 public void CanCreateIndexWithArrayOfNestedObjects()
		 {
			 using (var store = NewDocumentStore())
			 {
				 new ContainsIndex().Execute(store);
			 }
		 }
	}
}