using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;

namespace Raven.Client.Indexes
{
    public class AbstractResultsTransformer<TFrom> : AbstractIndexCreationTask
    {
		protected Expression<Func<IClientSideDatabase, IEnumerable<TFrom>, IEnumerable>> TransformResults { get; set; }

        public override IndexDefinition CreateIndexDefinition()
        {
            return new IndexDefinitionBuilder<TFrom>()
            {
                TransformResults = TransformResults
            }.ToIndexDefinition(Conventions ?? new DocumentConvention(), false);
        }
    }
}
