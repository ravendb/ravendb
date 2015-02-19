using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Client.Linq;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem
{
    public class AbstractFilesQuery<T, TSelf> : IAbstractFilesQuery<T>
        where TSelf : AbstractFilesQuery<T, TSelf>
        where T : class
    {
        private static readonly ILog log = LogManager.GetCurrentClassLogger();

        private readonly LinqPathProvider linqPathProvider;
        private readonly FilesConvention conventions;

        /// <summary>
        /// The query to use
        /// </summary>
        protected StringBuilder queryText = new StringBuilder();

        public FilesConvention Conventions
        {
            get { return conventions; }
        }

        private int currentClauseDepth;
        
        protected bool isDistinct;
        
        /// <summary>
        /// Whatever to negate the next operation
        /// </summary>
        protected bool negate;

        protected KeyValuePair<string, string> lastEquality;

        protected InMemoryFilesSessionOperations Session
        {
            get;
            private set;
        }

        protected IAsyncFilesCommands Commands
        {
            get;
            private set;
        }

        private int pageSize = 1024;
        private int start = 0;

        public AbstractFilesQuery(InMemoryFilesSessionOperations theSession, IAsyncFilesCommands commands)
        {
            this.conventions = theSession == null ? new FilesConvention() : theSession.Conventions;
            this.linqPathProvider = new LinqPathProvider(conventions);

            this.Session = theSession;
            this.Commands = commands;
        }

        /// <summary>
        ///   The fields to order the results by
        /// </summary>
        protected string[] orderByFields = new string[0];

        /// <summary>
        ///   Simplified method for opening a new clause within the query
        /// </summary>
        /// <returns></returns>
        public void OpenSubclause()
        {
            currentClauseDepth++;
            AppendSpaceIfNeeded(queryText.Length > 0 && queryText[queryText.Length - 1] != '(');
            NegateIfNeeded();
            queryText.Append("(");
        }

        /// <summary>
        ///   Simplified method for closing a clause within the query
        /// </summary>
        /// <returns></returns>
        public void CloseSubclause()
        {
            currentClauseDepth--;
            queryText.Append(")");
        }

        /// <summary>
        ///   Filter the results from the index using the specified where clause.
        /// </summary>
        /// <param name = "whereClause">The where clause.</param>
        public void Where(string whereClause)
        {
            AppendSpaceIfNeeded(queryText.Length > 0 && queryText[queryText.Length - 1] != '(');
            queryText.Append(whereClause);
        }

        /// <summary>
        ///   Matches exact value
        /// </summary>
        public void WhereEquals(string fieldName, object value)
        {
            WhereEquals(new WhereParams
            {
                FieldName = fieldName,
                Value = value
            });
        }       

        private void WhereEquals(WhereParams whereParams, bool isReversed)        
        {
            var fieldName = EnsureValidFieldName(whereParams, isReversed);
            var transformToEqualValue = TransformToEqualValue(whereParams, isReversed);

            lastEquality = new KeyValuePair<string, string>(fieldName, transformToEqualValue);

            AppendSpaceIfNeeded(queryText.Length > 0 && queryText[queryText.Length - 1] != '(');
            NegateIfNeeded();

            queryText.Append(RavenQuery.EscapeField(fieldName));
            queryText.Append(":");

            if (fieldName.EndsWith("_numeric"))
            {
                queryText.Append("[");
                queryText.Append(transformToEqualValue);
                queryText.Append(" TO ");
                queryText.Append(transformToEqualValue);
                queryText.Append("]");
            }
            else
            {
                queryText.Append(transformToEqualValue);
            }
        }

        public void WhereEquals(WhereParams whereParams)        
        {
            WhereEquals(whereParams, false);
        }

        /// <summary>
        ///   Matches fields which starts with the specified value.
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        public void WhereStartsWith(string fieldName, object value)
        {
            // NOTE: doesn't fully match StartsWith semantics
            WhereEquals(
                new WhereParams
                {
                    FieldName = fieldName,
                    Value = String.Concat(value, "*"),
                    IsAnalyzed = true,
                    AllowWildcards = true
                });
        }

        /// <summary>
        ///   Matches fields which ends with the specified value.
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        public void WhereEndsWith(string fieldName, object value)
        {
            // http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Wildcard%20Searches
            // You cannot use a * or ? symbol as the first character of a search

            // NOTE: doesn't fully match EndsWith semantics
            WhereEquals(
                new WhereParams
                {
                    FieldName = fieldName,
                    Value = String.Concat("*", value),
                    AllowWildcards = true,
                    IsAnalyzed = true
                }, true );
        }


        public void WhereIn(string fieldName, IEnumerable<object> values)
        {
            AppendSpaceIfNeeded(queryText.Length > 0 && char.IsWhiteSpace(queryText[queryText.Length - 1]) == false);
            NegateIfNeeded();

            var whereParams = new WhereParams
            {
                FieldName = fieldName
            };
            fieldName = EnsureValidFieldName(whereParams);

            var list = UnpackEnumerable(values).ToList();

            if (list.Count == 0)
            {
                queryText.Append("@emptyIn<")
                    .Append(RavenQuery.EscapeField(fieldName))
                    .Append(">:(no-results)");
                return;
            }

            queryText.Append("@in<")
                .Append(RavenQuery.EscapeField(fieldName))
                .Append(">:(");

            var first = true;
            AddItemToInClause(whereParams, list, first);
            queryText.Append(") ");
        }

        /// <summary>
        ///   Matches fields where the value is between the specified start and end, exclusive
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "start">The start.</param>
        /// <param name = "end">The end.</param>
        /// <returns></returns>
        public void WhereBetween(string fieldName, object start, object end)
        {
            AppendSpaceIfNeeded(queryText.Length > 0);

            NegateIfNeeded();

            fieldName = GetFieldNameForRangeQueries(fieldName, start, end);

            queryText.Append(RavenQuery.EscapeField(fieldName)).Append(":{");
            queryText.Append(start == null ? "*" : TransformToRangeValue(new WhereParams { Value = start, FieldName = fieldName }));
            queryText.Append(" TO ");
            queryText.Append(end == null ? "NULL" : TransformToRangeValue(new WhereParams { Value = end, FieldName = fieldName }));
            queryText.Append("}");
        }

        /// <summary>
        ///   Matches fields where the value is between the specified start and end, inclusive
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "start">The start.</param>
        /// <param name = "end">The end.</param>
        /// <returns></returns>
        public void WhereBetweenOrEqual(string fieldName, object start, object end)
        {
            AppendSpaceIfNeeded(queryText.Length > 0);

            NegateIfNeeded();

            fieldName = GetFieldNameForRangeQueries(fieldName, start, end);

            queryText.Append(RavenQuery.EscapeField(fieldName)).Append(":[");
            queryText.Append(start == null ? "*" : TransformToRangeValue(new WhereParams { Value = start, FieldName = fieldName }));
            queryText.Append(" TO ");
            queryText.Append(end == null ? "NULL" : TransformToRangeValue(new WhereParams { Value = end, FieldName = fieldName }));
            queryText.Append("]");
        }

        /// <summary>
        ///   Matches fields where the value is greater than the specified value
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        public void WhereGreaterThan(string fieldName, object value)
        {
            WhereBetween(fieldName, value, null);
        }

        /// <summary>
        ///   Matches fields where the value is greater than or equal to the specified value
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        public void WhereGreaterThanOrEqual(string fieldName, object value)
        {
            WhereBetweenOrEqual(fieldName, value, null);
        }

        /// <summary>
        ///   Matches fields where the value is less than the specified value
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        public void WhereLessThan(string fieldName, object value)
        {
            WhereBetween(fieldName, null, value);
        }

        /// <summary>
        ///   Matches fields where the value is less than or equal to the specified value
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        public void WhereLessThanOrEqual(string fieldName, object value)
        {
            WhereBetweenOrEqual(fieldName, null, value);
        }

        /// <summary>
        ///   Add an AND to the query
        /// </summary>
        public void AndAlso()
        {
            if (queryText.Length < 1)
                return;

            queryText.Append(" AND");
        }

        /// <summary>
        ///   Add an OR to the query
        /// </summary>
        public void OrElse()
        {
            if (queryText.Length < 1)
                return;

            queryText.Append(" OR");
        }

        public void Distinct()
        {
            isDistinct = true;
        }

        public void ContainsAny(string fieldName, IEnumerable<object> values)
        {
            ContainsAnyAllProcessor(fieldName, values, "OR");
        }

        public void ContainsAll(string fieldName, IEnumerable<object> values)
        {
            ContainsAnyAllProcessor(fieldName, values, "AND");
        }

        private void ContainsAnyAllProcessor(string fieldName, IEnumerable<object> values, string separator)
        {
            AppendSpaceIfNeeded(queryText.Length > 0 && char.IsWhiteSpace(queryText[queryText.Length - 1]) == false);
            NegateIfNeeded();

            var list = UnpackEnumerable(values).ToList();
            if (list.Count == 0)
            {
                return;
            }

            var first = true;
            queryText.Append("(");
            foreach (var value in list)
            {
                if (first == false)
                {
                    queryText.Append(" " + separator + " ");
                }
                first = false;
                var whereParams = new WhereParams
                {
                    AllowWildcards = true,
                    IsAnalyzed = true,
                    FieldName = fieldName,
                    Value = value
                };
                EnsureValidFieldName(whereParams);
                queryText.Append(fieldName)
                         .Append(":")
                         .Append(TransformToEqualValue(whereParams));
            }
            queryText.Append(")");
        }


        /// <summary>
        ///   Adds an ordering for a specific field to the query
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "descending">if set to <c>true</c> [descending].</param>
        public void AddOrder(string fieldName, bool descending)
        {
            fieldName = EnsureValidFieldName(new WhereParams
            {
                FieldName = fieldName,
            });
            fieldName = descending ? MakeFieldSortDescending(fieldName) : fieldName;
            orderByFields = orderByFields.Concat(new[] { fieldName }).ToArray();
        }

        /// <summary>
        ///   Order the results by the specified fields
        ///   The fields are the names of the fields to sort, defaulting to sorting by ascending.
        ///   You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
        /// </summary>
        /// <param name = "fields">The fields.</param>
        public void OrderBy(params string[] fields)
        {
            orderByFields = new string[0];
            foreach (var field in fields)
                AddOrder(field, false);
        }

        /// <summary>
        ///   Order the results by the specified fields
        ///   The fields are the names of the fields to sort, defaulting to sorting by descending.
        ///   You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
        /// </summary>
        /// <param name = "fields">The fields.</param>
        public void OrderByDescending(params string[] fields)
        {
            orderByFields = new string[0];
            foreach (var field in fields)
                AddOrder(field, true);
        }

        protected string MakeFieldSortDescending(string field)
        {
            if (string.IsNullOrWhiteSpace(field) || field.StartsWith("+") || field.StartsWith("-"))
            {
                return field;
            }

            return "-" + field;
        }

        /// <summary>
        ///   Returns a <see cref = "System.String" /> that represents the query for this instance.
        /// </summary>
        /// <returns>
        ///   A <see cref = "System.String" /> that represents the query for this instance.
        /// </returns>
        public override string ToString()
        {
            if (currentClauseDepth != 0)
                throw new InvalidOperationException(string.Format("A clause was not closed correctly within this query, current clause depth = {0}", currentClauseDepth));

            var queryString = queryText.ToString().Trim();

            if ( queryString.EndsWith("OR") )
                queryString = queryString.Substring(0, queryString.Length - 2);
            else if ( queryString.EndsWith("AND"))
                queryString = queryString.Substring(0, queryString.Length - 3);
            
            return queryString;
        }


        ///<summary>
        /// Negate the next operation
        ///</summary>
        public void NegateNext()
        {
            negate = !negate;
        }

        private void NegateIfNeeded()
        {
            if (negate == false)
                return;
            negate = false;
            queryText.Append("-");
        }

        private void AppendSpaceIfNeeded(bool shouldAppendSpace)
        {
            if (shouldAppendSpace)
                queryText.Append(" ");
        }

        private void AddItemToInClause(WhereParams whereParams, IEnumerable<object> list, bool first)
        {
            foreach (var value in list)
            {
                var enumerable = value as IEnumerable;
                if (enumerable != null && value is string == false)
                {
                    AddItemToInClause(whereParams, enumerable.Cast<object>(), first);
                    return;
                }
                if (first == false)
                {
                    queryText.Append(",");
                }
                first = false;
                var nestedWhereParams = new WhereParams
                {
                    AllowWildcards = true,
                    IsAnalyzed = true,
                    FieldName = whereParams.FieldName,
                    FieldTypeForIdentifier = whereParams.FieldTypeForIdentifier,
                    Value = value
                };
                queryText.Append(TransformToEqualValue(nestedWhereParams).Replace(",", "`,`"));
            }
        }

        private string EnsureValidFieldName(WhereParams whereParams, bool isReversed = false)
        {
            var prefix = "__";

            var term = whereParams.FieldName;                 
            if (term.StartsWith("Metadata."))
            {
                term = term.Substring(9);
                if (isReversed)
                    throw new NotSupportedException("StartWith and EndWith is not supported for metadata content.");
            }

            switch (term.ToLower())
            {
                case "name": term = "fileName"; break;
                case "directory": term = "directory"; break;   
                case "totalsize": term = "size"; break;
                case "lastmodified": term = "modified"; break;
                case "creationdate": term = "created"; break;
                case "etag": term = "etag"; break;
                case "path": term = "directory"; break;                
                case "extension": throw new NotSupportedException("Query over Extension is not supported yet, use Name instead.");
                case "humanetotalsize": throw new NotSupportedException("Query over HumaneTotalSize is not supported, use TotalSize instead.");
                case "originalmetadata": throw new NotSupportedException("Query over OriginalMetadata is not supported, use current Metadata instead.");
                default: prefix = string.Empty; break;
            }

            if (whereParams.Value is int || whereParams.Value is long || whereParams.Value is decimal)
                term = term + "_numeric";

            if (isReversed)
                prefix = prefix + "r";

            return prefix + term;
        }

        private bool UsesRangeType(object o)
		{
			if (o == null)
				return false;

			var type = o as Type ?? o.GetType();
			var nonNullable = Nullable.GetUnderlyingType(type);
			if (nonNullable != null)
				type = nonNullable;

			if (type == typeof (int) || type == typeof (long) || type == typeof (double) || type == typeof (float) ||
			    type == typeof (decimal) || type == typeof (TimeSpan) || type == typeof(short))
				return true;

			return false;
		}

        private string GetFieldNameForRangeQueries(string fieldName, object start, object end)
        {
            var val = (start ?? end);
            fieldName = EnsureValidFieldName(new WhereParams { FieldName = fieldName, Value = val });

            if (fieldName == Constants.DocumentIdFieldName)
                return fieldName;
           
            if (UsesRangeType(val) && fieldName.EndsWith("_Range"))
                fieldName = fieldName.Substring(0, fieldName.Length - 6);
            return fieldName;
        }


        public string GetMemberQueryPathForOrderBy(Expression expression)
        {
            return GetMemberQueryPath(expression);
        }

        public string GetMemberQueryPath(Expression expression)
        {
            var result = linqPathProvider.GetPath(expression);
			result.Path = result.Path.Substring(result.Path.IndexOf('.') + 1);

			if (expression.NodeType == ExpressionType.ArrayLength)
				result.Path += ".Length";

            return result.Path;
        }

        private string TransformToEqualValue(WhereParams whereParams, bool isReversed)
        {
            var result = TransformToEqualValue(whereParams);

            if (isReversed)
                return new string(result.Reverse().ToArray());
            else
                return result;
        }

        private string TransformToEqualValue(WhereParams whereParams)
        {
            if (whereParams.Value == null)
                return Constants.NullValueNotAnalyzed;

            if (Equals(whereParams.Value, string.Empty))
                return Constants.EmptyStringNotAnalyzed;

            var type = TypeSystem.GetNonNullableType(whereParams.Value.GetType());

            if (type == typeof(bool))
                return (bool)whereParams.Value ? "true" : "false";

            if (type == typeof(DateTime))
            {
                var val = (DateTime)whereParams.Value;
                var s = val.GetDefaultRavenFormat();
                if (val.Kind == DateTimeKind.Utc)
                    s += "Z";
                return s;
            }
            
            if (type == typeof(DateTimeOffset))
            {
                var val = (DateTimeOffset)whereParams.Value;
                return val.UtcDateTime.GetDefaultRavenFormat(true);
            }

            if (type == typeof(decimal))
                return RavenQuery.Escape(((double)((decimal)whereParams.Value)).ToString(CultureInfo.InvariantCulture), false, false);

            if (type == typeof(double))
                return RavenQuery.Escape(((double)(whereParams.Value)).ToString("r", CultureInfo.InvariantCulture), false, false);

            var strValue = whereParams.Value as string;
            if (strValue != null)
            {
                strValue = RavenQuery.Escape(strValue,
                        whereParams.AllowWildcards && whereParams.IsAnalyzed, true);

                return whereParams.IsAnalyzed ? strValue : String.Concat("[[", strValue, "]]");
            }

            if (whereParams.Value is ValueType)
            {
                var escaped = RavenQuery.Escape(Convert.ToString(whereParams.Value, CultureInfo.InvariantCulture),
                                                whereParams.AllowWildcards && whereParams.IsAnalyzed, true);

                return escaped;
            }

            var value = whereParams.Value as RavenJToken;
            if (value != null)
            {
                var term = value.ToString(Formatting.None).Trim('"');

                switch( value.Type )
                {
                    case JTokenType.Object:
                    case JTokenType.Array:
                        return "[[" + RavenQuery.Escape(term, false, false) + "]]";
                    default:
                        return RavenQuery.Escape(term, false, false);
                }
            }

            throw new NotSupportedException();    
        }

        private string TransformToRangeValue(WhereParams whereParams)
        {
            if (whereParams.Value == null)
                return Constants.NullValueNotAnalyzed;
            if (Equals(whereParams.Value, string.Empty))
                return Constants.EmptyStringNotAnalyzed;

            if (whereParams.Value is DateTime)
            {
                var dateTime = (DateTime)whereParams.Value;
                var dateStr = dateTime.GetDefaultRavenFormat(dateTime.Kind == DateTimeKind.Utc);
                return dateStr;
            }
            if (whereParams.Value is DateTimeOffset)
                return ((DateTimeOffset)whereParams.Value).UtcDateTime.GetDefaultRavenFormat(true);
            if (whereParams.Value is TimeSpan)
                return NumberUtil.NumberToString(((TimeSpan)whereParams.Value).Ticks);


            if (whereParams.Value is float)
                return NumberUtil.NumberToString((float)whereParams.Value);
            if (whereParams.Value is double)
                return NumberUtil.NumberToString((double)whereParams.Value);
            
            //TODO Change the server to recognize the different types.
            if (whereParams.Value is int)
                return whereParams.Value.ToString();
            if (whereParams.Value is long)
                return whereParams.Value.ToString();
            if (whereParams.Value is decimal)
                return whereParams.Value.ToString();

            if (whereParams.Value is string)
                return RavenQuery.Escape(whereParams.Value.ToString(), false, true);

            if (whereParams.Value is ValueType)
                return RavenQuery.Escape(Convert.ToString(whereParams.Value, CultureInfo.InvariantCulture), false, true);

            throw new NotSupportedException();
        }

        private IEnumerable<object> UnpackEnumerable(IEnumerable items)
        {
            foreach (var item in items)
            {
                var enumerable = item as IEnumerable;
                if (enumerable != null && item is string == false)
                {
                    foreach (var nested in UnpackEnumerable(enumerable))
                    {
                        yield return nested;
                    }
                }
                else
                {
                    yield return item;
                }
            }
        }


        protected async virtual Task<IEnumerable<T>> ExecuteActualQueryAsync()
        {
            Session.IncrementRequestCount();

            log.Debug("Executing query on file system '{0}' in '{1}'", this.Session.FileSystemName, this.Session.StoreIdentifier);

            var result = await Commands.SearchAsync(this.ToString(), this.orderByFields, start, pageSize);

            return result.Files.ConvertAll<T>(x => x as T);
        }

        /// <summary>
        ///   Takes the specified count.
        /// </summary>
        /// <param name = "count">The count.</param>
        /// <returns></returns>
        public void Take(int count)
        {
            pageSize = count;
        }

        /// <summary>
        ///   Skips the specified count.
        /// </summary>
        /// <param name = "count">The count.</param>
        /// <returns></returns>
        public void Skip(int count)
        {
            start = count;
        }


        public async Task<T> FirstOrDefaultAsync()
        {
            Take(1);

            var collection = await ExecuteActualQueryAsync();
            return collection.FirstOrDefault();
        }

        public async Task<T> FirstAsync()
        {
            Take(1);

            var collection = await ExecuteActualQueryAsync();
            return collection.First();
        }

        public async Task<T> SingleOrDefaultAsync()
        {
            Take(2);

            var collection = await ExecuteActualQueryAsync();
            return collection.SingleOrDefault();
        }

        public async Task<T> SingleAsync()
        {
            Take(2);

            var collection = await ExecuteActualQueryAsync();
            return collection.Single();
        }

        public async Task<List<T>> ToListAsync()
        {
            var collection = await ExecuteActualQueryAsync();
            return collection.ToList();
        }
    }
}
