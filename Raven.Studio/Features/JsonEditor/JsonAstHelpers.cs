using System.Collections.Generic;
using ActiproSoftware.Text;
using ActiproSoftware.Text.Parsing;
using ActiproSoftware.Text.Parsing.LLParser;

namespace Raven.Studio.Features.JsonEditor
{
    public static class JsonAstHelpers
    {
        public static IEnumerable<JsonStringNode> FindAllStringValueNodes(this ICodeDocument codeDocument)
        {
            var nodeQueue = new Queue<IAstNode>();

            var rootNode = GetRootAstNode(codeDocument);
			AddChildren(nodeQueue, rootNode);

            while (nodeQueue.Count > 0)
            {
                var node = nodeQueue.Dequeue();

                if (node is JsonStringNode)
                    yield return (JsonStringNode) node;
                else
                    AddChildren(nodeQueue, node);
            }
        }

        private static void AddChildren(Queue<IAstNode> nodeQueue, IAstNode node)
        {
            if (node is JsonObjectNode)
            {
                var objectNode = node as JsonObjectNode;
                foreach (var pairNode in objectNode.PropertyValues)
                {
                    nodeQueue.Enqueue(pairNode.Value);
                }
            }
            else if (node is JsonArrayNode)
            {
                var arrayNode = node as JsonArrayNode;
                foreach (var value in arrayNode.Values)
                {
                    nodeQueue.Enqueue(value);
                }
            }
        }

		private static JsonObjectNode GetRootAstNode(ICodeDocument document)
        {
            var parseData = document.ParseData as ILLParseData;
            if (parseData == null)
            {
                return null;
            }
	        var node = parseData.Ast.HasChildren ? parseData.Ast.Children[0] as JsonObjectNode : null;

	        return node;
        }
    }
}