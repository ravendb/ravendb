using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.Tokens;

namespace Raven.Client.Documents.Queries;

public interface IFilterFactory<T>
{
    /// <summary>
    ///     Matches value
    /// </summary>
    IFilterFactory<T> Equals(string fieldName, MethodCall value);

    /// <summary>
    ///     Matches value
    /// </summary>
    IFilterFactory<T> Equals<TValue>(Expression<Func<T, object>> propertySelector, TValue value);

    /// <summary>
    ///     Matches value
    /// </summary>
    IFilterFactory<T> Equals<TValue>(Expression<Func<T, object>> propertySelector, MethodCall value);

    /// <summary>
    ///     Matches value
    /// </summary>
    IFilterFactory<T> Equals(string fieldName, object value);
    
    /// <summary>
    ///     Matches value
    /// </summary>
    IFilterFactory<T> Equals(WhereParams WhereParams);

    /// <summary>
    ///     Not matches value
    /// </summary>
    IFilterFactory<T> NotEquals(string fieldName, object value);

    /// <summary>
    ///     Not matches the evaluated expression
    /// </summary>
    IFilterFactory<T> NotEquals(string fieldName, MethodCall value);

    /// <summary>
    ///     Not matches value
    /// </summary>
    IFilterFactory<T> NotEquals<TValue>(Expression<Func<T, object>> propertySelector, TValue value);

    /// <summary>
    ///     Matches value
    /// </summary>
    IFilterFactory<T> NotEquals<TValue>(Expression<Func<T, object>> propertySelector, MethodCall value);

    /// <summary>
    ///     Not matches value
    /// </summary>
    IFilterFactory<T> NotEquals(WhereParams WhereParams);

    /// <summary>
    ///     Matches fields where the value is greater than the specified value
    /// </summary>
    /// <param name="fieldName">Name of the field.</param>
    /// <param name="value">The value.</param>
    IFilterFactory<T> GreaterThan(string fieldName, object value);

    /// <summary>
    ///     Matches fields where the value is greater than the specified value
    /// </summary>
    /// <param name="propertySelector">Property selector for the field.</param>
    /// <param name="value">The value.</param>
    IFilterFactory<T> GreaterThan<TValue>(Expression<Func<T, object>> propertySelector, TValue value);

    /// <summary>
    ///     Matches fields Where the value is greater than or equal to the specified value
    /// </summary>
    /// <param name="fieldName">Name of the field.</param>
    /// <param name="value">The value.</param>
    IFilterFactory<T> GreaterThanOrEqual(string fieldName, object value);

    /// <summary>
    ///     Matches fields where the value is greater than or equal to the specified value
    /// </summary>
    /// <param name="propertySelector">Property selector for the field.</param>
    /// <param name="value">The value.</param>
    IFilterFactory<T> GreaterThanOrEqual<TValue>(Expression<Func<T, object>> propertySelector, TValue value);

    /// <summary>
    ///     Matches fields where the value is less than the specified value
    /// </summary>
    /// <param name="propertySelector">Property selector for the field.</param>
    /// <param name="value">The value.</param>
    IFilterFactory<T> LessThan(string fieldName, object value);

    /// <summary>
    ///     Matches fields where the value is less than the specified value
    /// </summary>
    /// <param name="propertySelector">Property selector for the field.</param>
    /// <param name="value">The value.</param>
    IFilterFactory<T> LessThan<TValue>(Expression<Func<T, object>> propertySelector, TValue value);

    /// <summary>
    ///     Matches fields where the value is less than or equal to the specified value
    /// </summary>
    /// <param name="fieldName">Name of the field.</param>
    /// <param name="value">The value.</param>
    IFilterFactory<T> LessThanOrEqual(string fieldName, object value);

    /// <summary>
    ///     Matches fields where the value is less than or equal to the specified value
    /// </summary>
    /// <param name="propertySelector">Property selector for the field.</param>
    /// <param name="value">The value.</param>
    IFilterFactory<T> LessThanOrEqual<TValue>(Expression<Func<T, object>> propertySelector, TValue value);
    
    /// <summary>
    ///     Simplified method for opening a new clause within the query
    /// </summary>
    /// <returns></returns>
    IFilterFactory<T> AndAlso();
    
    /// <summary>
    ///     Add an OR to the query
    /// </summary>
    IFilterFactory<T> OrElse();

    /// <summary>
    ///     Negate the next operation
    /// </summary>
    IFilterFactory<T> Not();
    
    /// <summary>
    ///     Simplified method for opening a new clause within the query
    /// </summary>
    /// <returns></returns>
    IFilterFactory<T> OpenSubclause();
    
    /// <summary>
    ///     Simplified method for closing a clause within the query
    /// </summary>
    /// <returns></returns>
    IFilterFactory<T> CloseSubclause();

}

internal class FilterFactory<T> : IFilterFactory<T>
{
    private IAbstractDocumentQuery<T> _documentQuery;

    public FilterFactory(IAbstractDocumentQuery<T> documentQuery, int filterLimit = int.MaxValue)
    {
        _documentQuery = documentQuery;
        SetFilterLimit(filterLimit);
    }

    /// <inheritdoc />
    public IFilterFactory<T> Equals(string fieldName, MethodCall value)
    {
        _documentQuery.WhereEquals(fieldName, value);
        return this;
    }

    /// <inheritdoc />
    public IFilterFactory<T> Equals<TValue>(Expression<Func<T, object>> propertySelector, TValue value)
    {
        _documentQuery.WhereEquals(GetFieldName(propertySelector), value);
        return this;
    }

    /// <inheritdoc />
    public IFilterFactory<T> Equals<TValue>(Expression<Func<T, object>> propertySelector, MethodCall value)
    {
        _documentQuery.WhereEquals(GetFieldName(propertySelector), value);
        return this;
    }
    
    /// <inheritdoc />
    public IFilterFactory<T> Equals(string fieldName, object value)
    {
        _documentQuery.WhereEquals(fieldName, value);
        return this;
    }

    /// <inheritdoc />
    public IFilterFactory<T> Equals(WhereParams WhereParams)
    {
        _documentQuery.WhereEquals(WhereParams);
        return this;
    }

    /// <inheritdoc />
    public IFilterFactory<T> NotEquals(string fieldName, object value)
    {
        _documentQuery.WhereNotEquals(fieldName, value);
        return this;
    }

    /// <inheritdoc />
    public IFilterFactory<T> NotEquals(string fieldName, MethodCall value)
    {
        _documentQuery.WhereNotEquals(fieldName, (object)value);
        return this;
    }

    /// <inheritdoc />
    public IFilterFactory<T> NotEquals<TValue>(Expression<Func<T, object>> propertySelector, TValue value)
    {
        _documentQuery.WhereNotEquals(GetFieldName(propertySelector), value);
        return this;
    }

    /// <inheritdoc />
    public IFilterFactory<T> NotEquals<TValue>(Expression<Func<T, object>> propertySelector, MethodCall value)
    {
        _documentQuery.WhereNotEquals(GetFieldName(propertySelector), value);
        return this;
    }

    /// <inheritdoc />
    public IFilterFactory<T> NotEquals(WhereParams WhereParams)
    {
        _documentQuery.WhereNotEquals(WhereParams);
        return this;
    }

    /// <inheritdoc />
    public IFilterFactory<T> GreaterThan(string fieldName, object value)
    {
        _documentQuery.WhereGreaterThan(fieldName, value);
        return this;
    }

    /// <inheritdoc />
    public IFilterFactory<T> GreaterThan<TValue>(Expression<Func<T, object>> propertySelector, TValue value)
    {
        _documentQuery.WhereGreaterThan(GetFieldName(propertySelector), value);
        return this;
    }

    /// <inheritdoc />
    public IFilterFactory<T> GreaterThanOrEqual(string fieldName, object value)
    {
        _documentQuery.WhereGreaterThanOrEqual(fieldName, value);
        return this;
    }

    /// <inheritdoc />
    public IFilterFactory<T> GreaterThanOrEqual<TValue>(Expression<Func<T, object>> propertySelector, TValue value)
    {
        _documentQuery.WhereGreaterThanOrEqual(GetFieldName(propertySelector), value);
        return this;
    }

    /// <inheritdoc />
    public IFilterFactory<T> LessThan(string fieldName, object value)
    {
        _documentQuery.WhereLessThan(fieldName, value);
        return this;
    }

    /// <inheritdoc />
    public IFilterFactory<T> LessThan<TValue>(Expression<Func<T, object>> propertySelector, TValue value)
    {
        _documentQuery.WhereLessThan(GetFieldName(propertySelector), value);
        return this;
    }

    /// <inheritdoc />
    public IFilterFactory<T> LessThanOrEqual(string fieldName, object value)
    {
        _documentQuery.WhereLessThanOrEqual(fieldName, value);
        return this;
    }

    /// <inheritdoc />
    public IFilterFactory<T> LessThanOrEqual<TValue>(Expression<Func<T, object>> propertySelector, TValue value)
    {
        _documentQuery.WhereLessThanOrEqual(GetFieldName(propertySelector), value);
        return this;
    }

    /// <inheritdoc />
    public IFilterFactory<T> AndAlso()
    {
        _documentQuery.AndAlso();
        return this;
    }
    /// <inheritdoc />
    public IFilterFactory<T> OrElse()
    {
        _documentQuery.OrElse();
        return this;
    }
    
    /// <inheritdoc />
    public IFilterFactory<T> Not()
    {
        _documentQuery.NegateNext();
        return this;
    }
    
    /// <inheritdoc />
    public IFilterFactory<T> OpenSubclause()
    {
        _documentQuery.OpenSubclause();
        return this;
    }
    
    /// <inheritdoc />
    public IFilterFactory<T> CloseSubclause()
    {
        _documentQuery.CloseSubclause();
        return this;
    }

    private string GetFieldName(Expression<Func<T, object>> propertySelector) => _documentQuery switch
    {
        AsyncDocumentQuery<T> asyncDocumentQuery => asyncDocumentQuery.GetMemberQueryPath(propertySelector.Body),
        DocumentQuery<T> documentQuery => documentQuery.GetMemberQueryPath(propertySelector.Body),
        _ => throw new ArgumentOutOfRangeException(nameof(_documentQuery))
    };
    
    private void SetFilterLimit(int limit)
    {
        switch (_documentQuery)
        {
            case AsyncDocumentQuery<T> asyncDocumentQuery:
                asyncDocumentQuery.AddFilterLimit(limit);
                break;
            case DocumentQuery<T> documentQuery:
                documentQuery.AddFilterLimit(limit);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(_documentQuery));
        }
    }
}
