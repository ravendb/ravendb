using System.Collections.Generic;
using Rhino.Etl.Core;
using Rhino.Etl.Core.Enumerables;
using Rhino.Etl.Core.Operations;
using Rhino.Etl.Core.Pipelines;

namespace Raven.StackOverflow.Etl.Generic
{
	public class SimplePipelineExecutor : AbstractPipelineExecuter
	{
		protected override IEnumerable<Row> DecorateEnumerableForExecution(IOperation operation, IEnumerable<Row> enumerator)
		{
			foreach (Row row in new EventRaisingEnumerator(operation, enumerator))
			{
				yield return row;
			}
		}
	}
}
