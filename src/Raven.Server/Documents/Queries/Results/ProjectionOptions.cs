using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;

namespace Raven.Server.Documents.Queries.Results
{
    public class ProjectionOptions
    {
        public readonly bool MustExtractFromIndex;

        public readonly bool MustExtractFromDocument;

        public readonly bool MustExtractOrThrow;

        private readonly IndexQueryServerSide _query;

        public ProjectionOptions(IndexQueryServerSide query)
        {
            _query = query ?? throw new System.ArgumentNullException(nameof(query));

            var mustExtractFromIndex = query.ProjectionBehavior.FromIndexOnly();
            var mustExtractFromDocument = query.ProjectionBehavior.FromDocumentOnly();
            var mustExtractOrThrow = (mustExtractFromIndex || mustExtractFromDocument) && query.ProjectionBehavior.MustThrow();

            MustExtractFromIndex = mustExtractFromIndex;
            MustExtractFromDocument = mustExtractFromDocument;
            MustExtractOrThrow = mustExtractOrThrow;
        }

        public void ThrowCouldNotExtractProjectionOnDocumentBecauseDocumentDoesNotExistException(string documentId)
        {
            throw new InvalidQueryException($"Could not execute projection on document '{documentId}', because document does not exist.", _query.Query, _query.QueryParameters);
        }

        public void ThrowCouldNotExtractFieldFromDocumentBecauseDocumentDoesNotExistException(string documentId, string fieldName)
        {
            throw new InvalidQueryException($"Could not extract field '{fieldName}' from document '{documentId}', because document does not exist.", _query.Query, _query.QueryParameters);
        }

        public void ThrowCouldNotExtractFieldFromDocumentBecauseDocumentDoesNotContainSuchField(string documentId, string fieldName)
        {
            throw new InvalidQueryException($"Could not extract field '{fieldName}' from document '{documentId}', because document does not contain such a field.", _query.Query, _query.QueryParameters);
        }

        public void ThrowCouldNotExtractFieldFromIndexBecauseIndexDoesNotContainSuchFieldOrFieldValueIsNotStored(string fieldName)
        {
            throw new InvalidQueryException($"Could not extract field '{fieldName}' from index '{_query.Metadata.IndexName}', because index does not contain such a field or field value is not stored within index.", _query.Query, _query.QueryParameters);
        }
    }
}
