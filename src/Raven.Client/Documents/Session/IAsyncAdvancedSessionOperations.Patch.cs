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
        /// Increments a numeric property of the specified entity by a given value.
        /// </summary>
        /// <typeparam name="T">The type of the entity.</typeparam>
        /// <typeparam name="U">The type of the numeric property.</typeparam>
        /// <param name="entity">The entity containing the property to be incremented.</param>
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
        void AddOrPatch<T, TU>(string id, T entity, Expression<Func<T, List<TU>>> path, Expression<Func<JavaScriptArray<TU>, object>> arrayAdder);

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
}
