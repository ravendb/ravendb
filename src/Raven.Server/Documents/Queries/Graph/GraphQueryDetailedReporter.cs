using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Queries.Graph
{
    public class GraphQueryDetailedReporter : QueryPlanVisitor
    {
        private AsyncBlittableJsonTextWriter _writer;
        private DocumentsOperationContext _ctx;

        public GraphQueryDetailedReporter(AsyncBlittableJsonTextWriter writer, DocumentsOperationContext ctx)
        {
            _writer = writer;
            _ctx = ctx;
        }

        public override void VisitQueryQueryStep(QueryQueryStep qqs)
        {
            _writer.WriteStartObject();
            _writer.WritePropertyName("Type");
            _writer.WriteString("QueryQueryStep");
            _writer.WriteComma();
            _writer.WritePropertyName("Query");
            _writer.WriteString(qqs.Query.ToString());
            _writer.WriteComma();
            WriteIntermidiateResults(qqs.IntermediateResults);
            _writer.WriteEndObject();
        }

        private void WriteIntermidiateResults(List<GraphQueryRunner.Match> matches)
        {
            _writer.WritePropertyName("Results");
            _writer.WriteStartArray();
            var first = true;
            foreach (var match in matches)
            {
                if (first == false)
                {
                    _writer.WriteComma();
                }

                first = false;
                var djv = new DynamicJsonValue();
                match.PopulateVertices(djv);
                _writer.WriteObject(_ctx.ReadObject(djv, null));
            }

            _writer.WriteEndArray();
        }

        public override async Task VisitEdgeQueryStepAsync(EdgeQueryStep eqs)
        {
            _writer.WriteStartObject();
            _writer.WritePropertyName("Type");
            _writer.WriteString("EdgeQueryStep");
            _writer.WriteComma();
            _writer.WritePropertyName("Left");
            await VisitAsync(eqs.Left);
            _writer.WriteComma();
            _writer.WritePropertyName("Right");
            await VisitAsync(eqs.Right);
            _writer.WriteEndObject();
        }

        public override void VisitCollectionDestinationQueryStep(CollectionDestinationQueryStep cdqs)
        {
            _writer.WriteStartObject();
            _writer.WritePropertyName("Type");
            _writer.WriteString("CollectionDestinationQueryStep");
            _writer.WriteComma();
            _writer.WritePropertyName("Collection");
            _writer.WriteString(cdqs.CollectionName);
            _writer.WriteComma();
            WriteIntermidiateResults(cdqs.IntermediateResults);
            _writer.WriteEndObject();
        }

        public override async Task VisitIntersectionQueryStepExceptAsync(IntersectionQueryStep<Except> iqse)
        {
            _writer.WriteStartObject();
            _writer.WritePropertyName("Type");
            _writer.WriteString("IntersectionQueryStep<Except>");
            _writer.WriteComma();
            _writer.WritePropertyName("Left");
            await VisitAsync(iqse.Left);
            _writer.WriteComma();
            _writer.WritePropertyName("Right");
            await VisitAsync(iqse.Right);
            _writer.WriteComma();
            WriteIntermidiateResults(iqse.IntermediateResults);
            _writer.WriteEndObject();
        }

        public override async Task VisitIntersectionQueryStepUnionAsync(IntersectionQueryStep<Union> iqsu)
        {
            _writer.WriteStartObject();
            _writer.WritePropertyName("Type");
            _writer.WriteString("IntersectionQueryStep<Except>");
            _writer.WriteComma();
            _writer.WritePropertyName("Left");
            await VisitAsync(iqsu.Left);
            _writer.WriteComma();
            _writer.WritePropertyName("Right");
            await VisitAsync(iqsu.Right);
            _writer.WriteComma();
            WriteIntermidiateResults(iqsu.IntermediateResults);
            _writer.WriteEndObject();
        }

        public override async Task VisitIntersectionQueryStepIntersectionAsync(IntersectionQueryStep<Intersection> iqsi)
        {
            _writer.WriteStartObject();
            _writer.WritePropertyName("Type");
            _writer.WriteString("IntersectionQueryStep<Except>");
            _writer.WriteComma();
            _writer.WritePropertyName("Left");
            await VisitAsync(iqsi.Left);
            _writer.WriteComma();
            _writer.WritePropertyName("Right");
            await VisitAsync(iqsi.Right);
            _writer.WriteComma();
            WriteIntermidiateResults(iqsi.IntermediateResults);
            _writer.WriteEndObject();
        }

        public override async Task VisitRecursionQueryStepAsync(RecursionQueryStep rqs)
        {
            _writer.WriteStartObject();
            _writer.WritePropertyName("Type");
            _writer.WriteString("RecursionQueryStep");
            _writer.WriteComma();
            _writer.WritePropertyName("Left");
            await VisitAsync(rqs.Left);
            _writer.WriteComma();
            _writer.WritePropertyName("Steps");
            _writer.WriteStartArray();
            var first = true;
            foreach (var step in rqs.Steps)
            {
                if (first == false)
                {
                    _writer.WriteComma();
                }

                first = false;
                await VisitAsync(step.Right);
            }
            _writer.WriteEndArray();
            _writer.WriteComma();
            await VisitAsync(rqs.GetNextStep());
            WriteIntermidiateResults(rqs.IntermediateResults);
            _writer.WriteEndObject();
        }

        public override async Task VisitEdgeMatcherAsync(EdgeQueryStep.EdgeMatcher em)
        {
            _writer.WritePropertyName("Next");
            await VisitAsync(em._parent.Right);
            _writer.WriteComma();
        }
    }
}
