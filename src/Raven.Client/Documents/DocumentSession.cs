//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Client.Connection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Data;
using Raven.Client.Document;
using Raven.Client.Documents.SessionOperations;
using Raven.Client.Http;

namespace Raven.Client.Documents
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public class DocumentSession : InMemoryDocumentSessionOperations
    {
        /// <summary>
        /// Gets the database commands.
        /// </summary>
        /// <value>The database commands.</value>
        public IDatabaseCommands DatabaseCommands { get; private set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="DocumentSession"/> class.
        /// </summary>
        public DocumentSession(string dbName, DocumentStore documentStore, DocumentSessionListeners listeners, Guid id, IDatabaseCommands databaseCommands, RequestExecuter requestExecuter)
            : base(dbName, documentStore, requestExecuter, id)
        {
            DatabaseCommands = databaseCommands;
        }

        /// <summary>
        /// Loads the specified entity with the specified id.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id">The id.</param>
        /// <returns></returns>
        public T Load<T>(string id)
        {
            var loadOeration = new LoadOperation(this);
            loadOeration.ById(id);

            var command = loadOeration.CreateRequest();
            if (command != null)
            {
                RequestExecuter.Execute(command, Context);
                loadOeration.SetResult(command.Result);
            }

            return loadOeration.GetDocument<T>();
        }

        /// <summary>
        /// Loads the specified entities with the specified ids.
        /// </summary>
        /// <param name="ids">The ids.</param>
        public T[] Load<T>(IEnumerable<string> ids)
        {
            var loadOeration = new LoadOperation(this);
            loadOeration.ByIds(ids);

            var command = loadOeration.CreateRequest();
            if (command != null)
            {
                RequestExecuter.Execute(command, Context);
                loadOeration.SetResult(command.Result);
            }

            return loadOeration.GetDocuments<T>();
        }

        /// <summary>
        /// Loads the specified entity with the specified id after applying
        /// conventions on the provided id to get the real document id.
        /// </summary>
        /// <remarks>
        /// This method allows you to call:
        /// Load{Post}(1)
        /// And that call will internally be translated to 
        /// Load{Post}("posts/1");
        /// 
        /// Or whatever your conventions specify.
        /// </remarks>
        public T Load<T>(ValueType id)
        {
            var documentKey = Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false);
            return Load<T>(documentKey);
        }

        /// <summary>
        /// Loads the specified entities with the specified id after applying
        /// conventions on the provided id to get the real document id.
        /// </summary>
        /// <remarks>
        /// This method allows you to call:
        /// Load{Post}(1,2,3)
        /// And that call will internally be translated to 
        /// Load{Post}("posts/1","posts/2","posts/3");
        /// 
        /// Or whatever your conventions specify.
        /// </remarks>
        public T[] Load<T>(params ValueType[] ids)
        {
            var documentKeys = ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
            return Load<T>(documentKeys);
        }

        /// <summary>
        /// Loads the specified entities with the specified id after applying
        /// conventions on the provided id to get the real document id.
        /// </summary>
        /// <remarks>
        /// This method allows you to call:
        /// Load{Post}(new List&lt;int&gt;(){1,2,3})
        /// And that call will internally be translated to 
        /// Load{Post}("posts/1","posts/2","posts/3");
        /// 
        /// Or whatever your conventions specify.
        /// </remarks>
        public T[] Load<T>(IEnumerable<ValueType> ids)
        {
            var documentKeys = ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
            return Load<T>(documentKeys);
        }

        protected override JsonDocument GetJsonDocument(string documentKey)
        {
            throw new NotImplementedException();
        }

        protected override string GenerateKey(object entity)
        {
            return Conventions.GenerateDocumentKey(databaseName, DatabaseCommands, entity);
        }

        protected override Task<string> GenerateKeyAsync(object entity)
        {
            throw new NotImplementedException();
        }
    }
}