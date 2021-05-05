//-----------------------------------------------------------------------
// <copyright file="IRevisionsSessionOperations.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Raven.Client.Json;

namespace Raven.Client.Documents.Session
{

    public enum ForceRevisionStrategy
    {
        // Don't force revision
        None,
        
        // Create a forced revision from the document that is currently in store - BEFORE applying any changes made by the user 
        // The only exception is: with current implementation the forced revision for *new* document only is 'after', not 'before'  
        Before
    }
    
    /// <summary>
    /// Revisions advanced synchronous session operations
    /// </summary>
    public interface IRevisionsSessionOperations
    {
        /// <summary>
        /// Returns all previous document revisions for specified document (with paging) ordered by most recent revision first.
        /// </summary>
        List<T> GetFor<T>(string id, int start = 0, int pageSize = 25);

        /// <summary>
        /// Returns all previous document revisions metadata for specified document (with paging).
        /// </summary>
        List<MetadataAsDictionary> GetMetadataFor(string id, int start = 0, int pageSize = 25);

        /// <summary>
        /// Returns a document revision by change vector.
        /// </summary>
        T Get<T>(string changeVector);

        /// <summary>
        /// Returns document revisions by change vectors.
        /// </summary>
        Dictionary<string, T> Get<T>(IEnumerable<string> changeVectors);

        /// <summary>
        /// Returns the first revision for this document that happens before or at
        /// the specified date
        /// </summary>
        T Get<T>(string id, DateTime date);
        
        /// <summary>
        /// Make the session create a revision for the specified entity.
        /// Can be used with tracked entities only.
        /// Revision will be created Even If:
        ///    1. Revisions configuration is Not set for the collection
        ///    2. Document was Not modified
        /// </summary>
        void ForceRevisionCreationFor<T>(T entity, ForceRevisionStrategy strategy = ForceRevisionStrategy.Before); 
        
        /// <summary>
        /// Make the session create a revision for the specified document id.
        /// Revision will be created Even If:
        ///    1. Revisions configuration is Not set for the collection
        ///    2. Document was Not modified
        /// </summary>
        /// <param name="id"></param>
        void ForceRevisionCreationFor(string id, ForceRevisionStrategy strategy = ForceRevisionStrategy.Before);

        /// <summary>
        /// Returns the number of revisions for specified document.
        /// </summary>
        long GetCountFor(string id);
        
        /// <summary>
        /// Access the lazy revisions operations 
        /// </summary>
        ILazyRevisionsOperations Lazily { get;}
    }
    /// <summary>
    /// Revisions advanced synchronous Lazy session operations
    /// </summary>
    public interface ILazyRevisionsOperations
    {
        /// <summary>
        /// Returns all previous document revisions for specified document (with paging) ordered by most recent revision first.
        /// </summary>
        Lazy<List<T>> GetFor<T>(string id, int start = 0, int pageSize = 25);
        
        /// <summary>
        /// Returns all previous document revisions metadata for specified document (with paging).
        /// </summary>
        Lazy<List<MetadataAsDictionary>> GetMetadataFor(string id, int start = 0, int pageSize = 25); 
        
        /// <summary>
        /// Returns a document revision by change vector.
        /// </summary>
        Lazy<T> Get<T>(string changeVector); 
        
        /// <summary>
        /// Returns document revisions by change vectors.
        /// </summary>
        Lazy<Dictionary<string, T>> Get<T>(IEnumerable<string> changeVectors);  
        
        /// <summary>
        /// Returns the first revision for this document that happens before or at the specified date.
        /// </summary>
        Lazy<T> Get<T>(string id, DateTime date); 
        
    }
}
