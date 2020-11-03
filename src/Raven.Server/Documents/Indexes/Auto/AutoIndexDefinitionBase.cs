using System;
using System.Linq;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.Documents.Indexes.Auto
{
    public abstract class AutoIndexDefinitionBase : IndexDefinitionBase<AutoIndexField>
    {
        public IndexState State { get; set; }

        protected AutoIndexDefinitionBase(string indexName, string collection, AutoIndexField[] fields, long? indexVersion = null)
            : base(indexName, new[] { collection }, IndexLockMode.Unlock, IndexPriority.Normal, fields, indexVersion ?? IndexVersion.CurrentVersion)
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

                writer.WritePropertyName(nameof(field.HasSuggestions));
                writer.WriteBool(field.HasSuggestions);

                writer.WriteEndObject();

                first = false;
            }
            writer.WriteEndArray();
        }

        protected internal abstract override IndexDefinition GetOrCreateIndexDefinitionInternal();

        public abstract override IndexDefinitionCompareDifferences Compare(IndexDefinitionBase indexDefinition);

        public abstract override IndexDefinitionCompareDifferences Compare(IndexDefinition indexDefinition);

        protected abstract override int ComputeRestOfHash(int hashCode);
    }
}
