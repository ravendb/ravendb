using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.AI
{
    /// <summary>
    /// A single prompt part sent by the client when running an AI conversation.
    /// </summary>
    public abstract class ContentPart(string type)
    {
        /// <summary>
        /// The prompt part type identifier.
        /// </summary>
        public string Type { get; } = type;

        /// <summary>
        /// Serializes the prompt part to a JSON structure.
        /// </summary>
        public abstract DynamicJsonValue ToJson();
    }

    /// <summary>
    /// A text prompt part.
    /// </summary>
    public sealed class TextPart : ContentPart
    {
        /// <summary>
        /// The text content.
        /// </summary>
        public string Text { get; set; }

        /// <summary>
        /// Initializes a new instance of <see cref="TextPart"/> with the specified <paramref name="text"/>.
        /// </summary>
        /// <param name="text">The text content.</param>
        public TextPart(string text) : base(AiMessagePromptTypes.Text)
        {
            this.Text = text;
        }

        /// <summary>
        /// Serializes the prompt part to a JSON structure.
        /// </summary>
        public override DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [AiMessagePromptFields.Type] = this.Type,
                [AiMessagePromptFields.Text] = this.Text
            };
        }
    }
}
