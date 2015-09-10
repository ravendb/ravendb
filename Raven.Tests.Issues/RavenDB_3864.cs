using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.ComponentModel.Composition.Hosting;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Raven.Tests.MailingList;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3864 : RavenTest
	{
		[Fact]
		public void CanUseConventionsWithCreatIndexes()
		{
			using (var store = NewDocumentStore())
			{

				var container = new CompositionContainer(new TypeCatalog(typeof (CustomIdInIndexCreationTask)));
				var Conventions = new DocumentConvention();
				Conventions.PrettifyGeneratedLinqExpressions = true;
				IndexCreation.CreateIndexes(container, store.DatabaseCommands, Conventions);	
				Assert.True(!testFailed);
			}

		}

		private static bool testFailed = false;
		[Export(typeof(AbstractIndexCreationTask<Data>))]
		public class CustomIdInIndexCreationTask: AbstractIndexCreationTask<Data> 
		{
			public CustomIdInIndexCreationTask()
			{
				Map = docs => from doc in docs select new { doc.CustomId };				
			}

			public override IndexDefinition CreateIndexDefinition()
			{
				if (Conventions == null || Conventions.PrettifyGeneratedLinqExpressions == false) testFailed = true;
				return base.CreateIndexDefinition();
			}
		}
		public class Data
		{
			public string CustomId { get; set; }
		}
	}
}
