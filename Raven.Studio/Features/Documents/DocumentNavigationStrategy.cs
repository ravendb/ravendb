using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Connection.Async;
using Raven.Client.Exceptions;
using Raven.Json.Linq;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;
using Raven.Studio.Framework;
using System.Linq;

namespace Raven.Studio.Features.Documents
{
    public class DocumentAndNavigationInfo
    {
        public JsonDocument Document { get; set; }
        public long Index { get; set; }
        public long TotalDocuments { get; set; }
        public IList<PathSegment> ParentPath { get; set; }
        public string UrlForFirst { get; set; }
        public string UrlForPrevious { get; set; }
        public string UrlForNext { get; set; }
        public string UrlForLast { get; set; }
        public bool IsConflicted { get { return ConflictingVersionIds != null && ConflictingVersionIds.Count > 0; } }
        public IList<string> ConflictingVersionIds { get; set; }
        public Etag ConflictEtag { get; set; }
        public string ConflictDocumentId { get; set; }
    }

    public class PathSegment
    {
        public string Name { get; set; }
        public string Url { get; set; }
    }

    public abstract class DocumentNavigator
    {
        public static DocumentNavigator Create(string id)
        {
            return new SingleDocumentNavigator(id);
        }

        public static DocumentNavigator Create(string id, int itemIndex)
        {
            return new AllDocumentsNavigator(id, itemIndex);
        }

        public static DocumentNavigator Create(string id, int itemIndex, string indexName, IndexQuery query)
        {
            return new IndexDocumentsNavigator(id, itemIndex, indexName, query);
        }

        public static DocumentNavigator FromUrl(UrlParser parser)
        {
            var mode = parser.GetQueryParam("navigationMode");

            if (mode == "allDocs")
                return AllDocumentsNavigator.AllDocumentsFromUrl(parser);
            
            if (mode == "index")
                return IndexDocumentsNavigator.IndexNavigatorFromUrl(parser);

            if (mode == "conflicts")
                return ConflictDocumentsNavigator.ConflictsNavigatorFromUrl(parser);

            return new SingleDocumentNavigator(parser.GetQueryParam("id"));
        }

        public abstract string GetUrl();

        public abstract Task<DocumentAndNavigationInfo> GetDocument();

        public virtual string GetUrlForNext()
        {
            return string.Empty;
        }

        public virtual string GetUrlForPrevious()
        {
            return string.Empty;
        }

        public virtual string GetUrlForCurrentIndex()
        {
            return string.Empty;
        }

        protected UrlParser GetBaseUrl()
        {
            return new UrlParser("/edit");
        }

        protected IAsyncDatabaseCommands DatabaseCommands
        {
            get { return ApplicationModel.DatabaseCommands; }
        }

        protected DocumentAndNavigationInfo PopulateDocumentOrConflictsFromTask(Task<JsonDocument> task, string documentId)
        {
            if (task.IsFaulted && task.Exception.GetBaseException() is ConflictException)
            {
                var conflictException = task.Exception.GetBaseException() as ConflictException;
                return new DocumentAndNavigationInfo
                {
                    ConflictingVersionIds = conflictException.ConflictedVersionIds,
                    ConflictEtag = conflictException.Etag,
                    ConflictDocumentId = documentId,
                };
            }

	        return new DocumentAndNavigationInfo()
	        {
		        Document = task.Result
	        };
        }

        protected DocumentAndNavigationInfo PopulateDocumentOrConflictsFromDocuments(IEnumerable<JsonDocument> documents)
        {
            var document = documents.FirstOrDefault();

            if (document == null)
            {
                return new DocumentAndNavigationInfo()
                {
                    Document = null
                };
            }
            
            if (document.Metadata.IfPresent<bool>(Constants.RavenReplicationConflict) && (document.Key.Length < 47 || !document.Key.Substring(document.Key.Length - 47).StartsWith("/conflicts/", StringComparison.OrdinalIgnoreCase)))
            {
                var idsArray = document.DataAsJson["Conflicts"] as RavenJArray;
                return new DocumentAndNavigationInfo()
                {
                    ConflictingVersionIds = idsArray.Values<string>().ToArray(),
                    ConflictEtag = document.Etag ?? Etag.Empty,
                    ConflictDocumentId = document.Key,
                };
            }

            return new DocumentAndNavigationInfo()
            {
                Document = document
            };
        }
    }
}