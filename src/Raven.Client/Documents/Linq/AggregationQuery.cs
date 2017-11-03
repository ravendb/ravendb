using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Extensions;
using Sparrow.Json;

namespace Raven.Client.Documents.Linq
{
    public class AggregationQuery<T>
    {
        private IQueryable<T> _source;
        private readonly Func<IQueryable<T>, Expression> _convertExpressionIfNecessary;
        private readonly MethodInfo _aggregateByMethod;

        public AggregationQuery(
            IQueryable<T> source,
            Func<IQueryable<T>, Expression> convertExpressionIfNecessary,
            MethodInfo aggregateByMethod)
        {
            _source = source;
            _convertExpressionIfNecessary = convertExpressionIfNecessary;
            _aggregateByMethod = aggregateByMethod;
        }

        public AggregationQuery<T> AndAggregateOn(Expression<Func<T, object>> path, Action<FacetFactory<T>> factory = null)
        {
            return AndAggregateOn(path.ToPropertyPath('_'), factory);
        }

        public AggregationQuery<T> AndAggregateOn(string path, Action<FacetFactory<T>> factory = null)
        {
            var f = new FacetFactory<T>();
            factory?.Invoke(f);
            f.Facet.Name = path;

            return AndAggregateOn(f.Facet);
        }

        public AggregationQuery<T> AndAggregateOn(Facet facet)
        {
            var expression = _convertExpressionIfNecessary(_source);
            _source = _source.Provider.CreateQuery<T>(Expression.Call(null, _aggregateByMethod.MakeGenericMethod(typeof(T)), expression, Expression.Constant(facet)));
            return this;
        }

        public Dictionary<string, FacetResult> ToDictionary()
        {
            var inspector = (IRavenQueryInspector)_source;
            var iq = inspector.GetIndexQuery(isAsync: false);

            var command = new QueryCommand(inspector.Session.Conventions, iq);

            inspector.Session.RequestExecutor.Execute(command, inspector.Session.Context);

            var results = new Dictionary<string, FacetResult>();
            foreach (BlittableJsonReaderObject result in command.Result.Results)
            {
                var facetResult = (FacetResult)EntityToBlittable.ConvertToEntity(typeof(FacetResult), "facet/result", result, inspector.Session.Conventions);
                results[facetResult.Name] = facetResult;
            }

            return results;
        }
        
        public Task<Dictionary<string, FacetResult>> ToDictionaryAsync()
        {
            throw new NotImplementedException();
        }

        public Lazy<Dictionary<string, FacetResult>> ToDictionaryLazy()
        {
            throw new NotImplementedException();
        }

        public Lazy<Task<Dictionary<string, FacetResult>>> ToDictionaryLazyAsync()
        {
            throw new NotImplementedException();
        }
    }
}
