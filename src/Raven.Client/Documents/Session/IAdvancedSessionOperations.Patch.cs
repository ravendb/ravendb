//-----------------------------------------------------------------------
// <copyright file="ISyncAdvancedSessionOperation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Advanced synchronous session operations
    /// </summary>
    public partial interface IAdvancedSessionOperations
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
        
        void AddOrPatch<T, TU>(string id, T entity, Expression<Func<T, List<TU>>> path, Expression<Func<JavaScriptArray<TU>,object>> arrayAdder);
        
        void AddOrIncrement<T, TU>(string id, T entity, Expression<Func<T, TU>> path, TU valToAdd);
    }

    public class JavascriptMethodNameAttribute : Attribute
    {
        public string Name { get; }

        public object[] PositionalArguments { get; set; }

        public JavascriptMethodNameAttribute(string name)
        {
            Name = name;
        }
    }

    public class JavaScriptArray<U>
    {

        [JavascriptMethodName("push")]
        public JavaScriptArray<U> Add(U u)
        {
            throw new NotSupportedException("Never called");
        }

        [JavascriptMethodName("push")]
        public JavaScriptArray<U> Add(params U[] u)
        {
            throw new NotSupportedException("Never called");
        }

        [JavascriptMethodName("splice", PositionalArguments = new object[] { 0, 1 })]
        public JavaScriptArray<U> RemoveAt(int index)
        {
            throw new NotSupportedException("Never called");
        }

        [JavascriptMethodName("filter")]
        public JavaScriptArray<U> RemoveAll(Func<U, bool> predicate)
        {
            throw new NotSupportedException("Never called");
        }
    }

    public class JavaScriptDictionary<TKey, TValue>
    {
        public JavaScriptDictionary<TKey, TValue> Add(TKey key, TValue value)
        {
            throw new NotSupportedException("Never called");
        }

        public JavaScriptDictionary<TKey, TValue> Add(KeyValuePair<TKey, TValue> kvp)
        {
            throw new NotSupportedException("Never called");
        }
        public JavaScriptDictionary<TKey, TValue> Remove(TKey key)
        {
            throw new NotSupportedException("Never called");
        }
    }
}
