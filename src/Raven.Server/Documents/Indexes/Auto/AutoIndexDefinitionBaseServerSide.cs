﻿using System;
using System.Linq;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Auto
{
    internal abstract class AutoIndexDefinitionBaseServerSide : IndexDefinitionBaseServerSide<AutoIndexField>
    {
        internal AutoIndexDefinitionBaseServerSide(string indexName, string collection, AutoIndexField[] fields, IndexDeploymentMode? deploymentMode,
            IndexDefinitionClusterState clusterState = null, long? indexVersion = null)
            : base(indexName, new[] {collection}, IndexLockMode.Unlock, IndexPriority.Normal, IndexState.Normal, fields, indexVersion ?? IndexVersion.CurrentVersion,
                deploymentMode, clusterState, archivedDataProcessingBehavior: null)
        {
            if (string.IsNullOrEmpty(collection))
                throw new ArgumentNullException(nameof(collection));
        }

        protected abstract override void PersistFields(JsonOperationContext context, AbstractBlittableJsonTextWriter writer);

        protected override void PersistMapFields(JsonOperationContext context, AbstractBlittableJsonTextWriter writer)
        {
            writer.WritePropertyName(nameof(MapFields));
            writer.WriteStartArray();
            var first = true;
            foreach (var field in MapFields.Values.Select(x => x.As<AutoIndexField>()))
            {
                if (first == false)
                    writer.WriteComma();

                writer.WriteStartObject();

                writer.WritePropertyName(nameof(field.Name));
                writer.WriteString(field.Name);
                writer.WriteComma();

                writer.WritePropertyName(nameof(field.Indexing));
                writer.WriteString(field.Indexing.ToString());
                writer.WriteComma();

                writer.WritePropertyName(nameof(field.Aggregation));
                writer.WriteInteger((int)field.Aggregation);
                writer.WriteComma();

                writer.WritePropertyName(nameof(field.Spatial));
                if (field.Spatial == null)
                    writer.WriteNull();
                else
                    writer.WriteObject(DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(field.Spatial, context));
                writer.WriteComma();

                writer.WritePropertyName(nameof(field.Vector));
                if (field.Vector == null)
                    writer.WriteNull();
                else
                    writer.WriteObject(DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(field.Vector, context));
                writer.WriteComma();
                
                writer.WritePropertyName(nameof(field.HasSuggestions));
                writer.WriteBool(field.HasSuggestions);
                writer.WriteComma();

                writer.WritePropertyName(nameof(field.HasQuotedName));
                writer.WriteBool(field.HasQuotedName);

                writer.WriteComma();

                writer.WriteEndObject();

                first = false;
            }
            writer.WriteEndArray();
        }

        protected internal abstract override IndexDefinition GetOrCreateIndexDefinitionInternal();

        public abstract override IndexDefinitionCompareDifferences Compare(IndexDefinitionBaseServerSide indexDefinition);

        public abstract override IndexDefinitionCompareDifferences Compare(IndexDefinition indexDefinition);

        protected abstract override int ComputeRestOfHash(int hashCode);
    }
}
