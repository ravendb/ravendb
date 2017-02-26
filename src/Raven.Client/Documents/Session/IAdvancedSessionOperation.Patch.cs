//-----------------------------------------------------------------------
// <copyright file="ISyncAdvancedSessionOperation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Lambda2Js;
using Newtonsoft.Json;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Advanced synchronous session operations
    /// </summary>
    public partial interface IAdvancedSessionOperation
    {
        void Increment<T, U>(T entity, Expression<Func<T, U>> path, U valToAdd);

        void Increment<T, U>(string key, Expression<Func<T, U>> path, U valToAdd);

        void Patch<T, U>(string key, Expression<Func<T, U>> path, U value);

        void Patch<T, U>(T entity, Expression<Func<T, U>> path, U value);

        void Patch<T, U>(T entity, Expression<Func<T, IEnumerable<U>>> path,
            Expression<Func<JavaScriptArray<U>, object>> arrayAdder);

        void Patch<T, U>(string key, Expression<Func<T, IEnumerable<U>>> path,
            Expression<Func<JavaScriptArray<U>, object>> arrayAdder);

    }

    public class JavaScriptArray<U>
    {

        [JavascriptMethodName("push")]
        public JavaScriptArray<U> Add(U u)
        {
            return null;
        }

        [JavascriptMethodName("concat")]
        public JavaScriptArray<U> Add(params U[] u)
        {
            return null;
        }

        [JavascriptMethodName("splice")]
        public JavaScriptArray<U> RemoveAt(int index)
        {
            return null;
        }
    }
}
