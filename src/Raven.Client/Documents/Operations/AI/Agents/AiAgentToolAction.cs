using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI.Agents
{
    /// <summary>
    /// Represents a tool action that can be invoked by an AI agent.
    /// Includes metadata such as name, description, and optional parameters schema or sample.
    /// Tool actions represent external functions whose results are provided by the user
    /// </summary>
    public class AiAgentToolAction : IDynamicJson
    {
        public AiAgentToolAction()
        {
            // for serialization
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AiAgentToolAction"/>.
        /// </summary>
        /// <param name="name">The name of the tool action. This should correspond to the function name exposed to the AI agent.</param>
        /// <param name="description">A brief description of what the tool action does. This is used by the AI model to understand when to invoke it.</param>
        public AiAgentToolAction(string name, string description)
        {
            ValidationMethods.AssertNotNullOrEmpty(name, nameof(name));
            ValidationMethods.AssertNotNullOrEmpty(description, nameof(description));

            Name = name;
            Description = description;
        }

        /// <summary>
        /// The name of the tool action.
        /// This is the function identifier that the AI uses when invoking the tool.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// The description of the tool action.
        /// Helps the AI understand when and why to use this action.
        /// </summary>
        public string Description { get; set; }

        /// <summary>
        /// A sample object representing the parameters for this tool.
        /// This should be a JSON-formatted string, showing an example of valid parameters.
        /// </summary>
        public string ParametersSampleObject { get; set; }

        /// <summary>
        /// The JSON schema for the parameters expected by this tool.
        /// This schema is used to validate and assist the AI in forming correct tool calls.
        /// </summary>
        public string ParametersSchema { get; set; }

        /// <summary>
        /// Serializes this tool action to JSON structure.
        /// </summary>
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Description)] = Description,
                [nameof(ParametersSampleObject)] = ParametersSampleObject,
                [nameof(ParametersSchema)] = ParametersSchema
            };
        }
    }

}
