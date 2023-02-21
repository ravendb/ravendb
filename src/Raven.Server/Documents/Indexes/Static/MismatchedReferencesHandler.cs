using System.Collections.Generic;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Static;

public class MismatchedReferencesHandler
{
    public Dictionary<string, Dictionary<string, LoadFailure>> MismatchedReferences;

    internal const int MaxMismatchedReferencesPerSource = 10;
    internal const int MaxMismatchedDocumentLoadsPerIndex = 10;
        
    internal bool _lastLoadMismatched = false;
    
    public class LoadFailure
    {
        public string SourceId;
        public string ReferenceId;
        public string ActualCollection;
        public HashSet<string> MismatchedCollections;
    }

    public MismatchedReferencesHandler()
    {
        MismatchedReferences = new Dictionary<string, Dictionary<string, LoadFailure>>();
    }
    
    public void HandleMismatchedReference(Document referencedDocument, string referencedCollectionName, LazyStringValue sourceId, string actualCollection)
    {
        // another mismatch for source document
        if(MismatchedReferences.TryGetValue(sourceId, out Dictionary<string, LoadFailure> mismatchesForDocument) && mismatchesForDocument.Count < MaxMismatchedReferencesPerSource)
        {
            // another mismatch referencing the same document
            if (mismatchesForDocument.TryGetValue(referencedDocument.Id, out LoadFailure loadFailure))
                loadFailure.MismatchedCollections.Add(referencedCollectionName);
            else
            {
                mismatchesForDocument.Add(
                    referencedDocument.Id, new LoadFailure()
                    {
                        SourceId = sourceId, 
                        ReferenceId = referencedDocument.Id,
                        ActualCollection = actualCollection,
                        MismatchedCollections = new HashSet<string>()
                        {
                            referencedCollectionName
                        } 
                    });
            }
        }
        else
        {
            // first mismatch for source document
            LoadFailure failure = new ()
            {
                SourceId = sourceId, 
                ReferenceId = referencedDocument.Id,
                ActualCollection = actualCollection,
                MismatchedCollections = new HashSet<string>()
                {
                    referencedCollectionName
                }
            };
                            
            MismatchedReferences.Add(sourceId, new Dictionary<string, LoadFailure>(){ {referencedDocument.Id, failure} });
        }
    }
    
    public void RemoveMismatchedReferenceOnMatchingLoad(Document document, string sourceId)
    {
        if (MismatchedReferences.TryGetValue(sourceId, out var failing) == false) 
            return;
            
        failing.Remove(document.Id);

        if (failing.Count == 0)
            MismatchedReferences.Remove(sourceId);

        if (MismatchedReferences.Count == 0)
            MismatchedReferences = new Dictionary<string, Dictionary<string, LoadFailure>>();
    }
}
