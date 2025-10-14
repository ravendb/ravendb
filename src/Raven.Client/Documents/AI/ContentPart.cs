using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.AI
{
    public abstract class ContentPart(string type)
    {
        public string Type { get; } = type;
        public abstract DynamicJsonValue ToJson();
    }

    public sealed class TextPart : ContentPart
    {
        public string Text { get; set; }

        public TextPart(string text) : base(AiMessagePromptTypes.Text)
        {
            this.Text = text;
        }
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
