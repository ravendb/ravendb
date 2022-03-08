//-----------------------------------------------------------------------
// <copyright file="IAsyncAdvancedSessionOperations.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Advanced async session operations
    /// </summary>
    public partial interface IAsyncAdvancedSessionOperations
    {
        void Increment<T, U>(T entity, Expression<Func<T, U>> path, U valToAdd);

        void Increment<T, U>(string id, Expression<Func<T, U>> path, U valToAdd);

        void Patch<T, U>(string id, Expression<Func<T, U>> path, U value);

        void Patch<T, U>(T entity, Expression<Func<T, U>> path, U value);

        void Patch<T, U>(T entity, Expression<Func<T, IEnumerable<U>>> path,
            Expression<Func<JavaScriptArray<U>, object>> arrayAdder);

        void Patch<T, U>(string id, Expression<Func<T, IEnumerable<U>>> path,
            Expression<Func<JavaScriptArray<U>, object>> arrayAdder);

        void Patch<T, TKey, TValue>(T entity, Expression<Func<T, IDictionary<TKey, TValue>>> path,
            Expression<Func<JavaScriptDictionary<TKey, TValue>, object>> dictionaryAdder);

        void Patch<T, TKey, TValue>(string id, Expression<Func<T, IDictionary<TKey, TValue>>> path,
            Expression<Func<JavaScriptDictionary<TKey, TValue>, object>> dictionaryAdder);

        void AddOrPatch<T, TU>(string id, T entity, Expression<Func<T, TU>> path, TU value);

        void AddOrPatch<T, TU>(string id, T entity, Expression<Func<T, List<TU>>> path, Expression<Func<JavaScriptArray<TU>, object>> arrayAdder);

        void AddOrIncrement<T, TU>(string id, T entity, Expression<Func<T, TU>> path, TU valToAdd);
    }
}
