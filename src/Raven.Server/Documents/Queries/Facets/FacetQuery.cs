using System;
using System.Collections.Generic;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Documents;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Queries.Facets
{
    public class FacetQuery
    {
        public readonly IndexQueryServerSide Query;

        public readonly Dictionary<string, FacetSetup> Facets;

        public readonly long FacetsEtag;

        private FacetQuery(IndexQueryServerSide query, Dictionary<string, FacetSetup> facets, long facetsEtag)
        {
            Query = query;
            Facets = facets;
            FacetsEtag = facetsEtag;
        }

        public static FacetQuery Create(DocumentsOperationContext context, IndexQueryServerSide query)
        {
            long? facetsEtag = null;
            DocumentsTransaction tx = null;
            try
            {
                var facets = new Dictionary<string, FacetSetup>(StringComparer.OrdinalIgnoreCase);
                foreach (var selectField in query.Metadata.SelectFields)
                {
                    if (selectField.IsFacet == false)
                        continue;

                    var facetField = (FacetField)selectField;
                    if (facetField.FacetSetupDocumentId == null || facets.ContainsKey(facetField.FacetSetupDocumentId))
                        continue;

                    if (tx == null)
                        tx = context.OpenReadTransaction();

                    var documentJson = context.DocumentDatabase.DocumentsStorage.Get(context, facetField.FacetSetupDocumentId);
                    if (documentJson == null)
                        throw new DocumentDoesNotExistException(facetField.FacetSetupDocumentId);

                    if (facetsEtag.HasValue == false)
                        facetsEtag = documentJson.Etag;
                    else
                        facetsEtag = facetsEtag.Value ^ documentJson.Etag;

                    var document = (FacetSetup)EntityToBlittable.ConvertToEntity(typeof(FacetSetup), facetField.FacetSetupDocumentId, documentJson.Data, DocumentConventions.DefaultForServer);

                    facets[facetField.FacetSetupDocumentId] = document;
                }

                return new FacetQuery(query, facets, facetsEtag ?? 0);
            }
            finally
            {
                tx?.Dispose();
            }
        }
    }
}
