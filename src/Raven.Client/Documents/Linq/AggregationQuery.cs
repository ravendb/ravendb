using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Session;
using Raven.Client.Extensions;
using Sparrow.Json;

namespace Raven.Client.Documents.Linq
{
    public class AggregationQuery<T> : AggregationQueryBase<T>
    {
        private readonly Func<IQueryable<T>, Expression> _convertExpressionIfNecessary;
        private readonly MethodInfo _aggregateByMethod;

        public AggregationQuery(
            IQueryable<T> source,
            Func<IQueryable<T>, Expression> convertExpressionIfNecessary,
            MethodInfo aggregateByMethod) : base(source)
        {
            _convertExpressionIfNecessary = convertExpressionIfNecessary;
            _aggregateByMethod = aggregateByMethod;
        }

        public AggregationQuery<T> AndAggregateOn(Expression<Func<T, object>> path, Action<FacetFactory<T>> factory = null)
        {
            return AndAggregateOn(path.ToPropertyPath('_'), factory);
        }

        public AggregationQuery<T> AndAggregateOn(string path, Action<FacetFactory<T>> factory = null)
        {
            var f = new FacetFactory<T>(path);
            factory?.Invoke(f);

            return AndAggregateOn(f.Facet);
        }

        public AggregationQuery<T> AndAggregateOn(Facet facet)
        {
            var expression = _convertExpressionIfNecessary(Source);
            Source = Source.Provider.CreateQuery<T>(Expression.Call(null, _aggregateByMethod.MakeGenericMethod(typeof(T)), expression, Expression.Constant(facet)));
            return this;
        }
    }

    public abstract class AggregationQueryBase<T>
    {
        protected IQueryable<T> Source;

        protected AggregationQueryBase(IQueryable<T> source)
        {
            Source = source;
        }

        public Dictionary<string, FacetResult> Execute()
        {
            var inspector = (IRavenQueryInspector)Source;
            var command = GetCommand(inspector, isAsync: false);

            inspector.Session.RequestExecutor.Execute(command, inspector.Session.Context);

            return GetResults(command, inspector.Session.Conventions);
        }

        public async Task<Dictionary<string, FacetResult>> ExecuteAsync()
        {
            var inspector = (IRavenQueryInspector)Source;
            var command = GetCommand(inspector, isAsync: true);

            await inspector.Session.RequestExecutor.ExecuteAsync(command, inspector.Session.Context).ConfigureAwait(false);

            return GetResults(command, inspector.Session.Conventions);
        }

        public Lazy<Dictionary<string, FacetResult>> ExecuteLazy()
        {
            return new Lazy<Dictionary<string, FacetResult>>(Execute);
        }

        public Lazy<Task<Dictionary<string, FacetResult>>> ExecuteLazyAsync()
        {
            return new Lazy<Task<Dictionary<string, FacetResult>>>(ExecuteAsync);
        }

        private static Dictionary<string, FacetResult> GetResults(QueryCommand command, DocumentConventions conventions)
        {
            var results = new Dictionary<string, FacetResult>();
            foreach (BlittableJsonReaderObject result in command.Result.Results)
            {
                var facetResult = (FacetResult)EntityToBlittable.ConvertToEntity(typeof(FacetResult), "facet/result", result, conventions);
                results[facetResult.Name] = facetResult;
            }

            return results;
        }

        private static QueryCommand GetCommand(IRavenQueryInspector inspector, bool isAsync)
        {
            var iq = inspector.GetIndexQuery(isAsync);

            return new QueryCommand(inspector.Session.Conventions, iq);
        }
    }
}
