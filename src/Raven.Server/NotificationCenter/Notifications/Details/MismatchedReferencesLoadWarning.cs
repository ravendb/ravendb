using System.Collections.Generic;
using Raven.Server.Documents.Indexes.Static;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details;

public sealed class MismatchedReferencesLoadWarning : INotificationDetails
{
    public Dictionary<string, List<WarningDetails>> Warnings { get; set; }
    
    public string IndexName { get; set; }

    public MismatchedReferencesLoadWarning()
    {
        // deserialization
    }
    
    public MismatchedReferencesLoadWarning(string indexName, Dictionary<string, Dictionary<string, MismatchedReferencesWarningHandler.LoadFailure>> mismatchedReferences)
    {
        IndexName = indexName;
        
        Warnings = new Dictionary<string, List<WarningDetails>>();

        foreach (var warningsForDocument in mismatchedReferences)
        {
            List<WarningDetails> warningsForDocumentList = new();
            
            foreach (var warning in warningsForDocument.Value)
            {
                warningsForDocumentList.Add(new WarningDetails(warning.Value));
            }
            
            Warnings.Add(warningsForDocument.Key, warningsForDocumentList);
        }
    }
    
    public DynamicJsonValue ToJson()
    {
        var djv = new DynamicJsonValue(GetType());

        var listOfWarnings = new DynamicJsonValue();

        foreach (var warning in Warnings)
        {
            var warningsForDocument = new DynamicJsonArray();
            
            foreach (var details in warning.Value)
            {
                warningsForDocument.Add( new DynamicJsonValue
                {
                    [nameof(WarningDetails.ReferenceId)] = details.ReferenceId,
                    [nameof(WarningDetails.SourceId)] = details.SourceId,
                    [nameof(WarningDetails.ActualCollection)] = details.ActualCollection,
                    [nameof(WarningDetails.MismatchedCollections)] = details.MismatchedCollections
                });
            }
            
            listOfWarnings[warning.Key] = warningsForDocument;
        }

        djv[nameof(Warnings)] = listOfWarnings;

        djv[nameof(IndexName)] = IndexName;

        return djv;
    }
    
    public sealed class WarningDetails
    {
        public string SourceId { get; set; }
        public string ReferenceId { get; set; }
        public string ActualCollection { get; set; }
        public HashSet<string> MismatchedCollections { get; set; }

        public WarningDetails()
        {
            // deserialization
        }
        
        public WarningDetails(MismatchedReferencesWarningHandler.LoadFailure loadFailure)
        {
            SourceId = loadFailure.SourceId;
            ReferenceId = loadFailure.ReferenceId;
            MismatchedCollections = loadFailure.MismatchedCollections;
            ActualCollection = loadFailure.ActualCollection;
        }
    }
}
