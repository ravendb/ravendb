using System;
using Raven.Client.Documents.Queries;

namespace Raven.Client.Documents.Session
{
    public class GroupByField
    {
        public string FieldName { get; set; }

        public string ProjectedName { get; set; }
    }

    public class GroupByDocumentQuery<T> : IGroupByDocumentQuery<T>
    {
        private readonly DocumentQuery<T> _query;

        public GroupByDocumentQuery(DocumentQuery<T> query)
        {
            _query = query;
        }

        public IGroupByDocumentQuery<T> SelectKey(string fieldName = null, string projectedName = null)
        {
            _query.GroupByKey(fieldName, projectedName);
            return this;
        }
        
        public IDocumentQuery<T> SelectSum(GroupByField field, params GroupByField[] fields)
        {
            if (field == null)
                throw new ArgumentNullException(nameof(field));

            _query.GroupBySum(field.FieldName, field.ProjectedName);

            if (fields == null || fields.Length == 0)
                return _query;

            foreach (var f in fields)
                _query.GroupBySum(f.FieldName, f.ProjectedName);

            return _query;
        }

        public IDocumentQuery<T> SelectCount(string projectedName = null)
        {
            _query.GroupByCount(projectedName);
            return _query;
        }
        
        public IGroupByDocumentQuery<T> Filter(Action<IFilterFactory<T>> builder, int limit)
        {
            using (_query.SetFilterMode(true))
            {
                var f = new FilterFactory<T>(_query, limit);
                builder.Invoke(f);
            }
            
            return this;
        }
    }

    public class AsyncGroupByDocumentQuery<T> : IAsyncGroupByDocumentQuery<T>
    {
        private readonly AsyncDocumentQuery<T> _query;

        public AsyncGroupByDocumentQuery(AsyncDocumentQuery<T> query)
        {
            _query = query;
        }

        public IAsyncGroupByDocumentQuery<T> SelectKey(string fieldName = null, string projectedName = null)
        {
            _query.GroupByKey(fieldName, projectedName);
            return this;
        }

        public IAsyncDocumentQuery<T> SelectSum(GroupByField field, params GroupByField[] fields)
        {
            if (field == null)
                throw new ArgumentNullException(nameof(field));

            _query.GroupBySum(field.FieldName, field.ProjectedName);

            if (fields == null || fields.Length == 0)
                return _query;

            foreach (var f in fields)
                _query.GroupBySum(f.FieldName, f.ProjectedName);

            return _query;
        }

        public IAsyncDocumentQuery<T> SelectCount(string projectedName = null)
        {
            _query.GroupByCount(projectedName);
            return _query;
        }
        
        public IAsyncGroupByDocumentQuery<T> Filter(Action<IFilterFactory<T>> builder, int limit)
        {
            using (_query.SetFilterMode(true))
            {
                var f = new FilterFactory<T>(_query, limit);
                builder.Invoke(f);
            }
           
            return this;
        }
    }
}
