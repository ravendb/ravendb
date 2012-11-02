using ActiproSoftware.Text;
using ActiproSoftware.Text.Parsing;
using ActiproSoftware.Text.Parsing.LLParser;
using ActiproSoftware.Windows.Controls.SyntaxEditor.Outlining.Implementation;

namespace Raven.Studio.Features.JsonEditor
{
    public class JsonOutliningSource : RangeOutliningSourceBase
    {
        private static OutliningNodeDefinition squareBraceDefinition;
        private static OutliningNodeDefinition curlyBraceDefinition;

        public JsonOutliningSource(ITextSnapshot snapshot) : base(snapshot)
        {
            Outline(snapshot);
        }

        private void Outline(ITextSnapshot snapshot)
        {
            var rootNode = GetRootAstNode(snapshot);
            if (rootNode != null)
            {
                ProcessJsonObjectPropertyValues(rootNode,snapshot);
            }
        }

        private void ProcessJsonObjectPropertyValues(JsonObjectNode rootNode, ITextSnapshot snapshot)
        {
            foreach (var jsonPropertyValuePair in rootNode.PropertyValues)
            {
                ProcessValue(snapshot, jsonPropertyValuePair.Value);
            }
        }

        private void ProcessValue(ITextSnapshot snapshot, IAstNode valueNode)
        {
            if (valueNode is JsonObjectNode)
            {
                ProcessJsonObject(snapshot, valueNode as JsonObjectNode);
            }
            else if (valueNode is JsonArrayNode)
            {
                AddOutliningNode(snapshot, valueNode, squareBraceDefinition);

                foreach (var node in (valueNode as JsonArrayNode).Values)
                {
                    ProcessValue(snapshot, node);
                }
            }
        }

        private void ProcessJsonObject(ITextSnapshot snapshot, JsonObjectNode jsonObject)
        {
            AddOutliningNode(snapshot, jsonObject, curlyBraceDefinition);
            ProcessJsonObjectPropertyValues(jsonObject, snapshot);
        }

        private void AddOutliningNode(ITextSnapshot snapshot, IAstNode node, OutliningNodeDefinition outliningNodeDefinition)
        {
            if (!node.StartOffset.HasValue || !node.EndOffset.HasValue)
                return;

            var startPosition = snapshot.OffsetToPosition(node.StartOffset.Value);
            var endPosition = snapshot.OffsetToPosition(node.EndOffset.Value);

            if (startPosition.Line != endPosition.Line)
                AddNode(new TextRange(node.StartOffset.Value + 1, node.EndOffset.Value - 1), outliningNodeDefinition);

        }

        private JsonObjectNode GetRootAstNode(ITextSnapshot snapshot)
        {
            var codeDoc = (snapshot.Document as ICodeDocument);
            if (codeDoc == null)
                return null;

            var parseData = codeDoc.ParseData as ILLParseData;
            if (parseData == null)
                return null;

            var node = parseData.Ast;

            if (node.HasChildren)
            {
                return node.Children[0] as JsonObjectNode;
            }
            
            return null;
        }

        static JsonOutliningSource()
        {
            squareBraceDefinition = new OutliningNodeDefinition("SquareBrace");

            curlyBraceDefinition = new OutliningNodeDefinition("CurlyBrace");
        }
    }
}