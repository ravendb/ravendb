export class AiAgentStubs {
    static getAiAgents(): GetAiAgentResultDto {
        return {
            AiAgents: [
                {
                    Identifier: "first-agent",
                    Name: "First agent",
                    ConnectionStringName: "open-ai",
                    SystemPrompt: "Sample prompt",
                    OutputSchema: "",
                    SampleObject: "{}",
                    Queries: [],
                    Actions: [],
                    Parameters: [],
                    ChatTrimming: null,
                    MaxModelIterationsPerCall: null,
                },
            ],
        };
    }
}
