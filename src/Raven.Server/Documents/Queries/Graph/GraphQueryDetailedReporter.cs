using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Documents.Session;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Queries.Graph
{
    public class GraphQueryDetailedReporter : QueryPlanVisitor
    {
        private BlittableJsonTextWriter _writer;
        private DocumentsOperationContext _ctx;

        public GraphQueryDetailedReporter(BlittableJsonTextWriter writer, DocumentsOperationContext ctx)
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

        public override void VisitEdgeQueryStep(EdgeQueryStep eqs)
        {
            _writer.WriteStartObject();
            _writer.WritePropertyName("Type");
            _writer.WriteString("EdgeQueryStep");
            _writer.WriteComma();
            _writer.WritePropertyName("Left");
            Visit(eqs.Left);
            _writer.WriteComma();
            _writer.WritePropertyName("Right");
            Visit(eqs.Right);
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

        public override void VisitIntersectionQueryStepExcept(IntersectionQueryStep<Except> iqse)
        {
            _writer.WriteStartObject();
            _writer.WritePropertyName("Type");
            _writer.WriteString("IntersectionQueryStep<Except>");
            _writer.WriteComma();
            _writer.WritePropertyName("Left");
            Visit(iqse.Left);
            _writer.WriteComma();
            _writer.WritePropertyName("Right");
            Visit(iqse.Right);
            _writer.WriteComma();
            WriteIntermidiateResults(iqse.IntermediateResults);
            _writer.WriteEndObject();
        }

        public override void VisitIntersectionQueryStepUnion(IntersectionQueryStep<Union> iqsu)
        {
            _writer.WriteStartObject();
            _writer.WritePropertyName("Type");
            _writer.WriteString("IntersectionQueryStep<Except>");
            _writer.WriteComma();
            _writer.WritePropertyName("Left");
            Visit(iqsu.Left);
            _writer.WriteComma();
            _writer.WritePropertyName("Right");
            Visit(iqsu.Right);
            _writer.WriteComma();
            WriteIntermidiateResults(iqsu.IntermediateResults);
            _writer.WriteEndObject();
        }

        public override void VisitIntersectionQueryStepIntersection(IntersectionQueryStep<Intersection> iqsi)
        {
            _writer.WriteStartObject();
            _writer.WritePropertyName("Type");
            _writer.WriteString("IntersectionQueryStep<Except>");
            _writer.WriteComma();
            _writer.WritePropertyName("Left");
            Visit(iqsi.Left);
            _writer.WriteComma();
            _writer.WritePropertyName("Right");
            Visit(iqsi.Right);
            _writer.WriteComma();
            WriteIntermidiateResults(iqsi.IntermediateResults);
            _writer.WriteEndObject();
        }

        public override void VisitRecursionQueryStep(RecursionQueryStep rqs)
        {
            _writer.WriteStartObject();
            _writer.WritePropertyName("Type");
            _writer.WriteString("RecursionQueryStep");
            _writer.WriteComma();
            _writer.WritePropertyName("Left");
            Visit(rqs.Left);
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
                Visit(step.Right);
            }
            _writer.WriteEndArray();
            _writer.WriteComma();
            Visit(rqs.GetNextStep());
            WriteIntermidiateResults(rqs.IntermediateResults);
            _writer.WriteEndObject();
        }

        public override void VisitEdgeMatcher(EdgeQueryStep.EdgeMatcher em)
        {
            _writer.WritePropertyName("Next");
            Visit(em._parent.Right);
            _writer.WriteComma();
        }
    }
}
