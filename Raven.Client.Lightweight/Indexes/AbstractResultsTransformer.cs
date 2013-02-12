using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace Raven.Client.Indexes
{
    public class AbstractResultsTransformer<TFrom>
    {
		protected Expression<Func<IClientSideDatabase, IEnumerable<TFrom>, IEnumerable>> TransformResults { get; set; }

    }
}
