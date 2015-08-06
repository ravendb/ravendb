using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3647 : RavenTest
	{
		[Fact]
		public void CanLockTransformers()
		{
			using (var store = NewDocumentStore())
			{
				store.ExecuteTransformer(new SimpleTransformer());
				//Checking that we can lock transformer
				store.DatabaseCommands.SetTransformerLock("SimpleTransformer",TransformerLockMode.LockedIgnore);
				var transformerDefinition = store.DatabaseCommands.GetTransformer("SimpleTransformer");
				var oldTransformResults = transformerDefinition.TransformResults;
				Assert.Equal(transformerDefinition.LockMode, TransformerLockMode.LockedIgnore);
				//Checking that we can't change a locked transformer
				transformerDefinition.TransformResults = newTransformResults;
				store.DatabaseCommands.PutTransformer("SimpleTransformer", transformerDefinition);
				transformerDefinition = store.DatabaseCommands.GetTransformer("SimpleTransformer");
				Assert.Equal(transformerDefinition.TransformResults, oldTransformResults);
				//Checking that we can unlock a transformer
				store.DatabaseCommands.SetTransformerLock("SimpleTransformer", TransformerLockMode.Unlock);
				transformerDefinition = store.DatabaseCommands.GetTransformer("SimpleTransformer");
				Assert.Equal(transformerDefinition.LockMode, TransformerLockMode.Unlock);
				//checking that the transformer is indeed overridden
				transformerDefinition.TransformResults = newTransformResults;
				store.DatabaseCommands.PutTransformer("SimpleTransformer", transformerDefinition);
				transformerDefinition = store.DatabaseCommands.GetTransformer("SimpleTransformer");
				Assert.Equal(transformerDefinition.TransformResults, newTransformResults);
			}
		}

		private const string newTransformResults = "from result in results  select new { Number = result.Number + int.MaxValue };";

		public class SimpleTransformer : AbstractTransformerCreationTask<SimpleData>
		{
			public SimpleTransformer()
			{
				TransformResults = results => from result in results
											  select new { Number = result.Number ^ int.MaxValue };
			}
		}
		public class SimpleData
		{
			public int Number { get; set; }
		}
	}
}
