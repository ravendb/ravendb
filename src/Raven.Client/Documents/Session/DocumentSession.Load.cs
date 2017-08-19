//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Session.Operations;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public partial class DocumentSession
    {
        /// <summary>
        /// Loads the specified entity with the specified id.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id">The id.</param>
        /// <returns></returns>
        public T Load<T>(string id)
        {
            if (id == null)
                return default(T);
            var loadOperation = new LoadOperation(this);
            loadOperation.ById(id);

            var command = loadOperation.CreateRequest();

            if (command != null)
            {
                RequestExecutor.Execute(command, Context, sessionId: _clientSessionId);
                loadOperation.SetResult(command.Result);
            }

            return loadOperation.GetDocument<T>();
        }

        /// <summary>
        /// Loads the specified entities with the specified ids.
        /// </summary>
        /// <param name="ids">The ids.</param>
        public Dictionary<string, T> Load<T>(IEnumerable<string> ids)
        {
            var loadOperation = new LoadOperation(this);
            LoadInternal(ids.ToArray(), loadOperation);
            return loadOperation.GetDocuments<T>();
        }

        private void LoadInternal(string[] ids, LoadOperation operation, Stream stream = null)
        {
            operation.ByIds(ids);

            var command = operation.CreateRequest();
            if (command != null)
            {
                RequestExecutor.Execute(command, Context, sessionId: _clientSessionId);
                if(stream!=null)
                    Context.Write(stream, command.Result.Results.Parent);
                else
                    operation.SetResult(command.Result);
            }
        }

        public Dictionary<string, T> LoadInternal<T>(string[] ids, string[] includes)
        {
            var loadOperation = new LoadOperation(this);
            loadOperation.ByIds(ids);
            loadOperation.WithIncludes(includes);

            var command = loadOperation.CreateRequest();
            if (command != null)
            {
                RequestExecutor.Execute(command, Context, sessionId: _clientSessionId);
                loadOperation.SetResult(command.Result);
            }

            return loadOperation.GetDocuments<T>();
        }

        public T[] LoadStartingWith<T>(string idPrefix, string matches = null, int start = 0, int pageSize = 25, string exclude = null,
            string startAfter = null)
        {
            var loadStartingWithOperation = new LoadStartingWithOperation(this);
            LoadStartingWithInternal(idPrefix, loadStartingWithOperation, null, matches, start, pageSize, exclude, startAfter);
            return loadStartingWithOperation.GetDocuments<T>();
        }


        public void LoadStartingWithIntoStream(string idPrefix, Stream output, string matches = null, int start = 0, int pageSize = 25, string exclude = null,
            string startAfter = null)
        {
            LoadStartingWithInternal(idPrefix, new LoadStartingWithOperation(this), output, matches, start, pageSize, exclude, startAfter);
        }

        private GetDocumentCommand LoadStartingWithInternal(string idPrefix, LoadStartingWithOperation operation, Stream stream = null, string matches = null,
            int start = 0, int pageSize = 25, string exclude = null, 
            string startAfter = null)
        {
            operation.WithStartWith(idPrefix, matches, start, pageSize, exclude, startAfter);

            var command = operation.CreateRequest();
            if (command != null)
            {
                RequestExecutor.Execute(command, Context, sessionId: _clientSessionId);

                if (stream != null)
                    Context.Write(stream, command.Result.Results.Parent);
                else
                    operation.SetResult(command.Result);
            }

            return command;
        }

        public void LoadIntoStream(IEnumerable<string> ids, Stream output)
        {
            LoadInternal(ids.ToArray(), new LoadOperation(this), output);
        }
    }
}
