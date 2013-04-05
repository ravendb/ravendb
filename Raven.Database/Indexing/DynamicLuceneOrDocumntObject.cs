// -----------------------------------------------------------------------
//  <copyright file="DynamicLuceneOrDocumntObject.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Linq;
using Raven.Database.Impl;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Database.Indexing
{
	[JsonObject]
    public class DynamicLuceneOrParentDocumntObject : DynamicJsonObject
    {
        private readonly DocumentRetriever retriever;
        private dynamic parentDoc;

        public DynamicLuceneOrParentDocumntObject(DocumentRetriever retriever,RavenJObject inner) : base(inner)
        {
            this.retriever = retriever;
        }

        public override object GetValue(string name)
        {
            var result = base.GetValue(name);
            if (result is DynamicNullObject == false)
                return result;

            if (parentDoc != null)
                return parentDoc[name];

            object documentId = GetDocumentId() as string;
            if (documentId == null)
                return result;

            parentDoc = retriever.Load(documentId);

            return parentDoc[name];
        }
    }
}