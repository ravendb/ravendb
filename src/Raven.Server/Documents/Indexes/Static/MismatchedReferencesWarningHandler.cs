using System.Collections.Generic;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Static;

public sealed class MismatchedReferencesWarningHandler
{
    private Dictionary<string, Dictionary<string, LoadFailure>> _mismatchedReferences;

    internal const int MaxMismatchedReferencesPerSource = 10;
    internal const int MaxMismatchedDocumentLoadsPerIndex = 10;
        
    public bool LastLoadMismatched { get; set; }
    public bool IsEmpty => _mismatchedReferences.Count == 0;
    public bool IsFull => _mismatchedReferences.Count >= MaxMismatchedDocumentLoadsPerIndex;

    public sealed class LoadFailure
    {
        public string SourceId;
        public string ReferenceId;
        public string ActualCollection;
        public HashSet<string> MismatchedCollections;
    }

    public MismatchedReferencesWarningHandler()
    {
        _mismatchedReferences = new Dictionary<string, Dictionary<string, LoadFailure>>();
    }

    public Dictionary<string, Dictionary<string, LoadFailure>> GetLoadFailures()
    {
        return _mismatchedReferences;
    }

    public void HandleMismatchedReference(Document referencedDocument, string referencedCollectionName, LazyStringValue sourceId, string actualCollection)
    {
        // another mismatch for source document
        if(_mismatchedReferences.TryGetValue(sourceId, out Dictionary<string, LoadFailure> mismatchesForDocument))
        {
            if (mismatchesForDocument.Count >= MaxMismatchedReferencesPerSource)
                return;
            
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
                            
            _mismatchedReferences.Add(sourceId, new Dictionary<string, LoadFailure>(){ {referencedDocument.Id, failure} });
        }
        
        LastLoadMismatched = true;
    }

    public void RemoveMismatchedReferenceOnMatchingLoad(Document document, string sourceId)
    {
        if (_mismatchedReferences.TryGetValue(sourceId, out var failing) == false)
            return;

        failing.Remove(document.Id);

        if (failing.Count == 0)
            _mismatchedReferences.Remove(sourceId);

        LastLoadMismatched = false;
    }
}
