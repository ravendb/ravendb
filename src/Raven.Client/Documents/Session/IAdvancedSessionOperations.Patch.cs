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
        /// <summary>
        /// Increments a numeric property of the specified entity by a given value.
        /// </summary>
        /// <typeparam name="T">The type of the entity.</typeparam>
        /// <typeparam name="U">The type of the numeric property.</typeparam>
        /// <param name="entity">The entity containing the property to be incremented.</param>
        /// <param name="path">An expression specifying the property to be incremented.</param>
        /// <param name="valToAdd">The value to add to the property.</param>
        void Increment<T, U>(T entity, Expression<Func<T, U>> path, U valToAdd);

        /// <summary>
        /// Increments a numeric property of a document by a given value.
        /// </summary>
        /// <typeparam name="T">The type of the entity.</typeparam>
        /// <typeparam name="U">The type of the numeric property.</typeparam>
        /// <param name="id">The ID of the document to update.</param>
        /// <param name="path">An expression specifying the property to be incremented.</param>
        /// <param name="valToAdd">The value to add to the property.</param>
        void Increment<T, U>(string id, Expression<Func<T, U>> path, U valToAdd);

        /// <summary>
        /// Updates a property of a document identified by its ID.
        /// </summary>
        /// <typeparam name="T">The type of the entity.</typeparam>
        /// <typeparam name="U">The type of the property.</typeparam>
        /// <param name="id">The ID of the document to update.</param>
        /// <param name="path">An expression specifying the property to update.</param>
        /// <param name="value">The new value to set for the property.</param>
        void Patch<T, U>(string id, Expression<Func<T, U>> path, U value);

        /// <summary>
        /// Updates a property of an entity and persists the change.
        /// </summary>
        /// <typeparam name="T">The type of the entity.</typeparam>
        /// <typeparam name="U">The type of the property.</typeparam>
        /// <param name="entity">The entity containing the property to update.</param>
        /// <param name="path">An expression specifying the property to update.</param>
        /// <param name="value">The new value to set for the property.</param>
        void Patch<T, U>(T entity, Expression<Func<T, U>> path, U value);

        /// <summary>
        /// Appends an item to a collection property of an entity and persists the change.
        /// </summary>
        /// <typeparam name="T">The type of the entity.</typeparam>
        /// <typeparam name="U">The type of the collection items.</typeparam>
        /// <param name="entity">The entity containing the collection property.</param>
        /// <param name="path">An expression specifying the collection property.</param>
        /// <param name="arrayAdder">An expression defining how to add an item to the collection.</param>
        void Patch<T, U>(T entity, Expression<Func<T, IEnumerable<U>>> path,
            Expression<Func<JavaScriptArray<U>, object>> arrayAdder);

        /// <summary>
        /// Appends an item to a collection property of a document identified by its ID.
        /// </summary>
        /// <typeparam name="T">The type of the entity.</typeparam>
        /// <typeparam name="U">The type of the collection items.</typeparam>
        /// <param name="id">The ID of the document to update.</param>
        /// <param name="path">An expression specifying the collection property.</param>
        /// <param name="arrayAdder">An expression defining how to add an item to the collection.</param>
        void Patch<T, U>(string id, Expression<Func<T, IEnumerable<U>>> path,
            Expression<Func<JavaScriptArray<U>, object>> arrayAdder);

        /// <summary>
        /// Modifies a dictionary property of an entity and persists the change.
        /// </summary>
        /// <typeparam name="T">The type of the entity.</typeparam>
        /// <typeparam name="TKey">The type of the dictionary keys.</typeparam>
        /// <typeparam name="TValue">The type of the dictionary values.</typeparam>
        /// <param name="entity">The entity containing the dictionary property.</param>
        /// <param name="path">An expression specifying the dictionary property.</param>
        /// <param name="dictionaryAdder">An expression defining the dictionary modification (e.g., add or remove a key-value pair).</param>
        void Patch<T, TKey, TValue>(T entity, Expression<Func<T, IDictionary<TKey, TValue>>> path,
            Expression<Func<JavaScriptDictionary<TKey, TValue>, object>> dictionaryAdder);

        /// <summary>
        /// Modifies a dictionary property of a document by adding or removing key-value pairs.
        /// </summary>
        /// <typeparam name="T">The type of the entity.</typeparam>
        /// <typeparam name="TKey">The type of the dictionary keys.</typeparam>
        /// <typeparam name="TValue">The type of the dictionary values.</typeparam>
        /// <param name="id">The ID of the document to update.</param>
        /// <param name="path">An expression specifying the dictionary property.</param>
        /// <param name="dictionaryAdder">An expression defining the dictionary modification (e.g., add or remove a key-value pair).</param>
        void Patch<T, TKey, TValue>(string id, Expression<Func<T, IDictionary<TKey, TValue>>> path,
            Expression<Func<JavaScriptDictionary<TKey, TValue>, object>> dictionaryAdder);

        /// <summary>
        /// Adds a new document if it does not exist, or updates a property of an existing document.
        /// </summary>
        /// <typeparam name="T">The type of the entity.</typeparam>
        /// <typeparam name="TU">The type of the property.</typeparam>
        /// <param name="id">The ID of the document.</param>
        /// <param name="entity">The entity to add if it does not exist.</param>
        /// <param name="path">An expression specifying the property to update.</param>
        /// <param name="value">The new value to set for the property.</param>
        void AddOrPatch<T, TU>(string id, T entity, Expression<Func<T, TU>> path, TU value);

        /// <summary>
        /// Adds or edits array in a single document.
        /// If the document doesn't yet exist, this operation adds the document but doesn't patch it.
        /// </summary>
        /// <typeparam name="T">The type of the entity.</typeparam>
        /// <typeparam name="TU">The type of the list items.</typeparam>
        /// <param name="id">The ID of the document.</param>
        /// <param name="entity">The entity to add if it does not exist.</param>
        /// <param name="path">An expression specifying the list property to modify.</param>
        /// <param name="arrayAdder">An expression defining how to add an item to the list.</param>
        void AddOrPatch<T, TU>(string id, T entity, Expression<Func<T, List<TU>>> path, Expression<Func<JavaScriptArray<TU>,object>> arrayAdder);

        /// <summary>
        /// Increments an existing field or adds a new one in documents where they didn't exist.
        /// </summary>
        /// <typeparam name="T">The type of the entity.</typeparam>
        /// <typeparam name="TU">The type of the numeric property.</typeparam>
        /// <param name="id">The ID of the document.</param>
        /// <param name="entity">The entity to add if it does not exist.</param>
        /// <param name="path">An expression specifying the property to be incremented.</param>
        /// <param name="valueToAdd">The value to add to the property.</param>
        void AddOrIncrement<T, TU>(string id, T entity, Expression<Func<T, TU>> path, TU valToAdd);
    }

    internal sealed class JavascriptMethodNameAttribute : Attribute
    {
        public string Name { get; }

        public object[] PositionalArguments { get; set; }

        public JavascriptMethodNameAttribute(string name)
        {
            Name = name;
        }
    }

    /// <summary>
    /// Represents a JavaScript-like array to be used within patch operations in RavenDB.
    /// This class is never actually executed; it is used to generate JavaScript code from LINQ expressions.
    /// </summary>
    /// <typeparam name="U">The type of the elements in the array.</typeparam>
    public sealed class JavaScriptArray<U>
    {
        /// <summary>
        /// Represents the JavaScript <c>push</c> method to add an element to the array.
        /// Not meant to be called directly.
        /// </summary>
        /// <param name="u">The element to add.</param>
        [JavascriptMethodName("push")]
        public JavaScriptArray<U> Add(U u)
        {
            throw new NotSupportedException("Never called");
        }

        /// <summary>
        /// Represents the JavaScript <c>push</c> method to add multiple elements to the array.
        /// Not meant to be called directly.
        /// </summary>
        /// <param name="u">The elements to add.</param>
        [JavascriptMethodName("push")]
        public JavaScriptArray<U> Add(params U[] u)
        {
            throw new NotSupportedException("Never called");
        }

        /// <summary>
        /// Represents the JavaScript <c>splice(0, 1)</c> method to remove an element at a specific index.
        /// Not meant to be called directly.
        /// </summary>
        /// <param name="index">The index of the element to remove.</param>
        [JavascriptMethodName("splice", PositionalArguments = new object[] { 0, 1 })]
        public JavaScriptArray<U> RemoveAt(int index)
        {
            throw new NotSupportedException("Never called");
        }

        /// <summary>
        /// Represents the JavaScript <c>filter</c> method to remove all elements matching a condition.
        /// Not meant to be called directly.
        /// </summary>
        /// <param name="predicate">The condition used to filter elements to remove.</param>
        [JavascriptMethodName("filter")]
        public JavaScriptArray<U> RemoveAll(Func<U, bool> predicate)
        {
            throw new NotSupportedException("Never called");
        }
    }

    /// <summary>
    /// Represents a JavaScript-like dictionary to be used within patch operations in RavenDB.
    /// This class is never actually executed; it is used to generate JavaScript code from LINQ expressions.
    /// </summary>
    /// <typeparam name="TKey">The type of the dictionary keys.</typeparam>
    /// <typeparam name="TValue">The type of the dictionary values.</typeparam>
    public sealed class JavaScriptDictionary<TKey, TValue>
    {
        /// <summary>
        /// Represents the JavaScript assignment to add or update a key-value pair.
        /// Not meant to be called directly.
        /// </summary>
        /// <param name="key">The key to add or update.</param>
        /// <param name="value">The value to associate with the key.</param>
        public JavaScriptDictionary<TKey, TValue> Add(TKey key, TValue value)
        {
            throw new NotSupportedException("Never called");
        }

        /// <summary>
        /// Represents the JavaScript assignment to add or update a key-value pair using a KeyValuePair.
        /// Not meant to be called directly.
        /// </summary>
        /// <param name="kvp">The key-value pair to add or update.</param>
        /// <returns>This method always throws <see cref="NotSupportedException"/>.</returns>
        public JavaScriptDictionary<TKey, TValue> Add(KeyValuePair<TKey, TValue> kvp)
        {
            throw new NotSupportedException("Never called");
        }

        /// <summary>
        /// Represents the JavaScript <c>delete</c> operation to remove a key from the dictionary.
        /// Not meant to be called directly.
        /// </summary>
        /// <param name="key">The key to remove.</param>
        public JavaScriptDictionary<TKey, TValue> Remove(TKey key)
        {
            throw new NotSupportedException("Never called");
        }
    }
}
