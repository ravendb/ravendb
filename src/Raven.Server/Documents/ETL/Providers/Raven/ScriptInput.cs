using System;
using System.Collections.Generic;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.Patch;

namespace Raven.Server.Documents.ETL.Providers.Raven
{

    public class ScriptInput
    {
        private readonly Dictionary<string, Dictionary<string, bool>> _collectionNameComparisons;

        public readonly string[] LoadToCollections = new string[0];

        public readonly PatchRequest Transformation;

        public readonly PatchRequest BehaviorFunctions;

        public readonly HashSet<string> DefaultCollections;

        public readonly Dictionary<string, string> IdPrefixForCollection = new Dictionary<string, string>();

        private readonly Dictionary<string, string> _collectionToLoadCounterBehaviorFunction;
        
        private readonly Dictionary<string, string> _collectionToLoadTimeSeriesBehaviorFunction;

        private readonly Dictionary<string, string> _collectionToDeleteDocumentBehaviorFunction;

        public bool HasTransformation => Transformation != null;

        public bool HasLoadCounterBehaviors => _collectionToLoadCounterBehaviorFunction != null;
        
        public bool HasLoadTimeSeriesBehaviors => _collectionToLoadTimeSeriesBehaviorFunction != null;

        public bool HasDeleteDocumentsBehaviors => _collectionToDeleteDocumentBehaviorFunction != null;

        public ScriptInput(Transformation transformation)
        {
            DefaultCollections = new HashSet<string>(transformation.Collections, StringComparer.OrdinalIgnoreCase);

            if (string.IsNullOrWhiteSpace(transformation.Script))
                return;

            if (transformation.IsEmptyScript == false)
                Transformation = new PatchRequest(transformation.Script, PatchRequestType.RavenEtl);

            if (transformation.Counters.CollectionToLoadBehaviorFunction != null)
                _collectionToLoadCounterBehaviorFunction = transformation.Counters.CollectionToLoadBehaviorFunction;

            if (transformation.TimeSeries.CollectionToLoadBehaviorFunction != null)
                _collectionToLoadTimeSeriesBehaviorFunction = transformation.TimeSeries.CollectionToLoadBehaviorFunction;

            if (transformation.CollectionToDeleteDocumentsBehaviorFunction != null)
                _collectionToDeleteDocumentBehaviorFunction = transformation.CollectionToDeleteDocumentsBehaviorFunction;

            if (HasLoadCounterBehaviors || HasDeleteDocumentsBehaviors || HasLoadTimeSeriesBehaviors)
                BehaviorFunctions = new PatchRequest(transformation.Script, PatchRequestType.EtlBehaviorFunctions);

            if (transformation.IsEmptyScript == false)
                LoadToCollections = transformation.GetCollectionsFromScript();

            foreach (var collection in LoadToCollections)
            {
                IdPrefixForCollection[collection] = DocumentConventions.DefaultTransformCollectionNameToDocumentIdPrefix(collection);
            }

            if (transformation.Collections == null)
                return;

            _collectionNameComparisons = new Dictionary<string, Dictionary<string, bool>>(transformation.Collections.Count);

            foreach (var sourceCollection in transformation.Collections)
            {
                _collectionNameComparisons[sourceCollection] = new Dictionary<string, bool>(transformation.Collections.Count);

                foreach (var loadToCollection in LoadToCollections)
                {
                    _collectionNameComparisons[sourceCollection][loadToCollection] = string.Compare(sourceCollection, loadToCollection, StringComparison.OrdinalIgnoreCase) == 0;
                }
            }
        }

        public bool TryGetLoadCounterBehaviorFunctionFor(string collection, out string functionName)
        {
            if(HasLoadCounterBehaviors)
                return _collectionToLoadCounterBehaviorFunction.TryGetValue(collection, out functionName);
            functionName = null;
            return false;
        }
        
        public bool TryGetLoadTimeSeriesBehaviorFunctionFor(string collection, out string functionName)
        {
            if (HasLoadTimeSeriesBehaviors) 
                return _collectionToLoadTimeSeriesBehaviorFunction.TryGetValue(collection, out functionName);
            functionName = null;
            return false;
        }

        public bool TryGetDeleteDocumentBehaviorFunctionFor(string collection, out string functionName)
        {
            return _collectionToDeleteDocumentBehaviorFunction.TryGetValue(collection, out functionName);
        }

        public bool MayLoadToDefaultCollection(RavenEtlItem item, string loadToCollection)
        {
            if (item.Collection != null)
                return _collectionNameComparisons[item.Collection][loadToCollection];

            var collection = item.CollectionFromMetadata;

            return collection?.CompareTo(loadToCollection) == 0;
        }
        
        public bool MayLoadToDefaultCollection(RavenEtlItem item)
        {
            if (item.Collection == null)
                return false;
            
            foreach (string loadToCollection in LoadToCollections)
            { 
                if(_collectionNameComparisons.ContainsKey(loadToCollection))
                    return true;
            }

            return false;
        }
    }

    public enum OperationType
    {
        Put,
        Delete
    }
}
