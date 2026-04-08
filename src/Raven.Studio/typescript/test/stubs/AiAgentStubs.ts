import document from "models/database/documents/document";

export class AiAgentStubs {
    static getAiAgents(): GetAiAgentResultDto {
        return {
            AiAgents: [
                {
                    Identifier: "first-agent",
                    Name: "First agent",
                    ConnectionStringName: "open-ai",
                    SystemPrompt:
                        "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.",
                    OutputSchema: "",
                    SampleObject:
                        '{\r\n    "Answer": "Answer to the user question",\r\n    "Relevant": true,\r\n    "RelevantOrdersId": ["The order ids relevant to the query or response"],\r\n    "MatchingProductsId": ["All the product ids referenced either by the user or the system"]\r\n}',
                    Queries: [
                        {
                            Name: "QueryProductSearch",
                            Description: "semantic search the store product catalog",
                            Query: "from Products where vector.search(embedding.text(Name), $query)",
                            ParametersSampleObject: '{"query": ["term or phrase to search in the catalog"]}',
                            ParametersSchema: null,
                            Options: {
                                AddToInitialContext: null,
                                AllowModelQueries: null,
                            },
                        },
                        {
                            Name: "QueryRecentCategories",
                            Description: "Get the recent orders of the current user",
                            Query: "from Categories",
                            ParametersSampleObject: "{}",
                            ParametersSchema: null,
                            Options: {
                                AddToInitialContext: null,
                                AllowModelQueries: null,
                            },
                        },
                        {
                            Name: "TEST_parameters",
                            Description: "Test",
                            Query: "from @all_docs as doc\r\nselect {\r\n  anyParam: $anyParam,\r\n  stringParam: $stringParam,\r\n  numberParam: $numberParam,\r\n  numberArray: $numberArrayLongNameNumbersVeryLongNameNumbersVeryLongNameNumbersVeryLongNameNumbersVeryLongNameNumbersVeryLongNameNumbersVeryLongNameNumbersVeryLongNameNumbersVeryLongNameNumbersVeryLongName,\r\n  boolParam: $boolParam,\r\n  stringArrayParam: $stringArrayParam,\r\n  boolArrayParam: $boolArrayParam,\r\n  nullParam: $nullParam,\r\n  testLLMParam_number: $testLLMParam_number,\r\n  testLLMParam_numberArray: $testLLMParam_numberArray\r\n}\r\nlimit 1\r\n",
                            ParametersSampleObject:
                                '{\n  "testLLMParam_string": "string",\n  "testLLMParam_stringArray": ["string1", "string2"],\n  "testLLMParam_number": 123,\n  "testLLMParam_numberArray": [1,2,3],\n  "testLLMParam_bool": true,\n  "testLLMParam_boolArray": [true, false]\n}',
                            ParametersSchema: null,
                            Options: {
                                AllowModelQueries: null,
                                AddToInitialContext: null,
                            },
                        },
                    ],
                    Actions: [
                        {
                            Name: "ActionProductSearch",
                            Description: "semantic search the store product catalog",
                            ParametersSampleObject: '{"query": ["term or phrase to search in the catalog"]}',
                            ParametersSchema: null,
                        },
                        {
                            Name: "ActionRecentOrder",
                            Description: "Get the recent orders of the current user",
                            ParametersSampleObject: "{}",
                            ParametersSchema: null,
                        },
                    ],
                    SubAgents: [
                        {
                            Identifier: "raven-expert-agent",
                            Description:
                                "An expert in RavenDB, who can answer any question about RavenDB and its features.",
                        },
                    ],
                    Parameters: [
                        {
                            Name: "anyParam",
                            Description: "Some long descritpion ",
                            SendToModel: false,
                            Policy: "Default",
                            Type: "Default",
                        },
                        {
                            Name: "stringParam",
                            Description: null,
                            SendToModel: false,
                            Policy: "Default",
                            Type: "String",
                        },
                        {
                            Name: "numberParam",
                            Description: "Currently used server version of RavenDB",
                            SendToModel: true,
                            Policy: "Default",
                            Type: "Number",
                        },
                        {
                            Name: "numberArrayLongNameNumbersVeryLongNameNumbersVeryLongNameNumbersVeryLongNameNumbersVeryLongNameNumbersVeryLongNameNumbersVeryLongNameNumbersVeryLongNameNumbersVeryLongNameNumbersVeryLongName",
                            Description:
                                "Some long descritpion Some long descritpion Some long descritpion Some long descritpion Some long descritpion Some long descritpion Some long descritpion Some long descritpion \nSome long descritpion Some long descritpion Some long descritpion Some long descritpion Some long descritpion Some long descritpion Some long descritpion Some long descritpion \nSome long descritpion Some long descritpion Some long descritpion Some long descritpion Some long descritpion Some long descritpion Some long descritpion Some long descritpion \nSome long descritpion Some long descritpion Some long descritpion Some long descritpion Some long descritpion Some long descritpion Some long descritpion Some long descritpion \nSome long descritpion Some long descritpion Some long descritpion Some long descritpion Some long descritpion Some long descritpion Some long descritpion Some long descritpion ",
                            SendToModel: true,
                            Policy: "ForbidModelGeneration",
                            Type: "ArrayOfNumber",
                        },
                        {
                            Name: "boolParam",
                            Description: null,
                            SendToModel: true,
                            Policy: "Default",
                            Type: "Boolean",
                        },
                        {
                            Name: "stringArrayParam",
                            Description: null,
                            SendToModel: true,
                            Policy: "Default",
                            Type: "ArrayOfString",
                        },
                        {
                            Name: "boolArrayParam",
                            Description: null,
                            SendToModel: true,
                            Policy: "Default",
                            Type: "ArrayOfBoolean",
                        },
                        {
                            Name: "nullParam",
                            Description: null,
                            SendToModel: true,
                            Policy: "Default",
                            Type: "Null",
                        },
                    ],
                    ChatTrimming: null,
                    MaxModelIterationsPerCall: null,
                    Disabled: false,
                },
            ],
        };
    }

    static getAiAgentDocument(): document {
        return new document({
            Agent: "first-agent",
            Parameters: {
                anyParam: {
                    Value: "any value",
                    SendToModel: false,
                },
                stringParam: {
                    Value: "some string",
                    SendToModel: false,
                },
                numberParam: {
                    Value: 7.1,
                    SendToModel: true,
                },
                numberArrayLongNameNumbersVeryLongNameNumbersVeryLongNameNumbersVeryLongNameNumbersVeryLongNameNumbersVeryLongNameNumbersVeryLongNameNumbersVeryLongNameNumbersVeryLongNameNumbersVeryLongName:
                    {
                        Value: [1, 2, 3, 4],
                        SendToModel: true,
                    },
                boolParam: {
                    Value: true,
                    SendToModel: true,
                },
                stringArrayParam: {
                    Value: ["aa", "bb"],
                    SendToModel: true,
                },
                boolArrayParam: {
                    Value: [true, false],
                    SendToModel: true,
                },
                nullParam: {
                    Value: null,
                    SendToModel: true,
                },
            },
            Messages: [
                {
                    role: "system",
                    content:
                        "You are an AI agent of an online shop, helping customers answer queries about that topic only. When talking about orders or products, include the ids as well.",
                    date: "2025-08-08T10:28:20.5582526Z",
                },
                {
                    role: "assistant",
                    content:
                        "Summary of previous conversation: - The conversation began with the user saying “Hello,” and the assistant responded politely with “Hello! How can I help you today?”\n- The user then requested: “Answer with some long text.” The assistant complied with a long, imaginative, narrative-style story about a mountain village and an explorer searching for a legendary library of every season. The story included advice from an old shepherd and encounters with a merchant, poet, and mechanic, and ended with the reveal that the library contained blank pages meant for the visitor to write their own chapter. The assistant closed by offering to make the text more poetic, more formal, or much longer.\n- The user then said “thanks,” and the assistant replied briefly and warmly: “You're welcome! If you'd like, I can also help with a new question or continue the conversation.”\n- The user then issued a reminder to go over the entire previous conversation and summarize it according to the original instructions, emphasizing that the final summary should include any essential tool results for future turns.\n- No tools were called at any point, so there are no external IDs, codes, prices, dates, or other tool outputs to preserve.\n\nResults Cache:\n- None (no tools were used).",
                    date: "2026-04-08T11:48:12.4936949Z",
                    usage: {
                        PromptTokens: 730,
                        CompletionTokens: 300,
                        TotalTokens: 1030,
                        CachedTokens: 0,
                        ReasoningTokens: 0,
                    },
                },
                {
                    role: "user",
                    content: "AI Agent Parameters:\ncompany = companies/90-A\r\n",
                    date: "2025-08-08T10:28:20.5582755Z",
                },
                {
                    role: "user",
                    content: "use QueryRecentCategories tool",
                    date: "2025-08-08T10:28:20.5582920Z",
                },
                {
                    role: "user",
                    content: "[Attachments: screen.png, screen(1).png]",
                    date: "2025-08-08T10:28:20.5582921Z",
                },
                {
                    role: "assistant",
                    content: null,
                    tool_calls: [
                        {
                            id: "call_whzFC5Mlx17thYJYOvdWf7RW",
                            type: "function",
                            function: {
                                name: "QueryRecentCategories",
                                arguments: "{}",
                            },
                        },
                        {
                            id: "call_whzFC5Mlx17thYJYOvdWf7RZ",
                            type: "function",
                            function: {
                                name: "TEST_parameters",
                                arguments:
                                    '{"testLLMParam_string":"7.1","testLLMParam_stringArray":["aa","bb"],"testLLMParam_number":7,"testLLMParam_numberArray":[1,2,3,4],"testLLMParam_bool":true,"testLLMParam_boolArray":[true,false]}',
                            },
                        },
                    ],
                    refusal: null,
                    annotations: [],
                    date: "2025-08-08T10:28:23.5363531Z",
                    usage: {
                        PromptTokens: 378,
                        CompletionTokens: 14,
                        TotalTokens: 412,
                        CachedTokens: 0,
                        ReasoningTokens: 20,
                    },
                },
                {
                    tool_call_id: "call_whzFC5Mlx17thYJYOvdWf7RW",
                    role: "tool",
                    content:
                        '[{"Name":"Beverages","Description":"Soft drinks, coffees, teas, beers, and ales","@metadata":{"@attachments":[{"Name":"image.jpg","Hash":"S5Opbm22FH1LW5SAC3wRb3HA64QM7odd26djlt5cAkM=","ContentType":"image/jpeg","Size":16958}],"@collection":"Categories","@change-vector":"A:1750-zbpmCj4aA0WLV/H8tjaiag","@flags":"HasAttachments","@id":"categories/1-A","@last-modified":"2018-07-27T12:15:47.7253469Z"}},{"Name":"Condiments","Description":"Sweet and savory sauces, relishes, spreads, and seasonings","@metadata":{"@attachments":[{"Name":"image.jpg","Hash":"YNLL8N+arOV1ZBP5q0wkeWc8RugEQ7wx3wRhB+xQWaI=","ContentType":"image/jpeg","Size":36514}],"@collection":"Categories","@change-vector":"A:1753-zbpmCj4aA0WLV/H8tjaiag","@flags":"HasAttachments","@id":"categories/2-A","@last-modified":"2018-07-27T12:16:24.1438586Z"}},{"Name":"Confections","Description":"Desserts, candies, and sweet breads","@metadata":{"@attachments":[{"Name":"image.jpg","Hash":"1QxSMa3tBr+y8wQYNre7E9UJFFVTNWGjVoC+IC+gSSs=","ContentType":"image/jpeg","Size":47955}],"@collection":"Categories","@change-vector":"A:1756-zbpmCj4aA0WLV/H8tjaiag","@flags":"HasAttachments","@id":"categories/3-A","@last-modified":"2018-07-27T12:16:44.1738714Z"}},{"Name":"Dairy Products","Description":"Cheeses","@metadata":{"@attachments":[{"Name":"image.jpg","Hash":"zBO1hw5HSdn8UYmWJKIXZdn2fdH0QNfzmPU2gSMc5yg=","ContentType":"image/jpeg","Size":43504}],"@collection":"Categories","@change-vector":"A:1759-zbpmCj4aA0WLV/H8tjaiag","@flags":"HasAttachments","@id":"categories/4-A","@last-modified":"2018-07-27T12:17:33.8212726Z"}},{"Name":"Grains/Cereals","Description":"Breads, crackers, pasta, and cereal","@metadata":{"@attachments":[{"Name":"image.jpg","Hash":"EMviKh017Gl7KUZWRecVbuCcXNQcrQ/7EdtnLKt/fgc=","ContentType":"image/jpeg","Size":55376}],"@collection":"Categories","@change-vector":"A:1762-zbpmCj4aA0WLV/H8tjaiag","@flags":"HasAttachments","@id":"categories/5-A","@last-modified":"2018-07-27T12:20:31.8237074Z"}},{"Name":"Meat/Poultry","Description":"Prepared meats","@metadata":{"@attachments":[{"Name":"image.jpg","Hash":"K37huqcfGCjDC0up0zVte7DAut5YS5K1z1kC+iUmeCI=","ContentType":"image/jpeg","Size":31219}],"@collection":"Categories","@change-vector":"A:1765-zbpmCj4aA0WLV/H8tjaiag","@flags":"HasAttachments","@id":"categories/6-A","@last-modified":"2018-07-27T12:20:49.7774078Z"}},{"Name":"Produce","Description":"Dried fruit and bean curd","@metadata":{"@attachments":[{"Name":"image.jpg","Hash":"asY7yUHhdgaVoKhivgua0OUSJKXqNDa3Z1uLP9XAocM=","ContentType":"image/jpeg","Size":61749}],"@collection":"Categories","@change-vector":"A:1768-zbpmCj4aA0WLV/H8tjaiag","@flags":"HasAttachments","@id":"categories/7-A","@last-modified":"2018-07-27T12:21:11.2283909Z"}},{"Name":"Seafood","Description":"Seaweed and fish","@metadata":{"@attachments":[{"Name":"image.jpg","Hash":"GWdpGVCWyLsrtNdA5AOee0QOZFG6rKIqCosZZN5WnCA=","ContentType":"image/jpeg","Size":33396}],"@collection":"Categories","@change-vector":"A:1771-zbpmCj4aA0WLV/H8tjaiag","@flags":"HasAttachments","@id":"categories/8-A","@last-modified":"2018-07-27T12:21:39.1315788Z"}}]',
                    date: "2025-08-08T10:28:23.5380397Z",
                },
                {
                    role: "assistant",
                    content:
                        '{"Answer":"I ran QueryRecentCategories for company companies/90-A and found 8 recent categories:\\n1) Beverages (categories/1-A) — Soft drinks, coffees, teas, beers, and ales\\n2) Condiments (categories/2-A) — Sweet and savory sauces, relishes, spreads, and seasonings\\n3) Confections (categories/3-A) — Desserts, candies, and sweet breads\\n4) Dairy Products (categories/4-A) — Cheeses\\n5) Grains/Cereals (categories/5-A) — Breads, crackers, pasta, and cereal\\n6) Meat/Poultry (categories/6-A) — Prepared meats\\n7) Produce (categories/7-A) — Dried fruit and bean curd\\n8) Seafood (categories/8-A) — Seaweed and fish","Relevant":true,"RelevantOrdersId":[],"MatchingProductsId":["categories/1-A","categories/2-A","categories/3-A","categories/4-A","categories/5-A","categories/6-A","categories/7-A","categories/8-A"]}',
                    refusal: null,
                    annotations: [],
                    date: "2025-08-08T10:28:30.5884757Z",
                    usage: {
                        PromptTokens: 1528,
                        CompletionTokens: 665,
                        TotalTokens: 2213,
                        CachedTokens: 0,
                        ReasoningTokens: 20,
                    },
                },
                {
                    content: null,
                    role: "assistant",
                    tool_calls: [
                        {
                            function: {
                                arguments: '{"Query":["test"]}',
                                name: "ActionProductSearch",
                            },
                            id: "call_MdKvWaFtl0cJAc5a0q26Lo98",
                            type: "function",
                        },
                    ],
                    date: "2025-08-08T10:28:31.5884757Z",
                },
                {
                    tool_call_id: "call_MdKvWaFtl0cJAc5a0q26Lo98",
                    role: "tool",
                    content: "Submitted content",
                    date: "2025-08-08T10:28:32.5884757Z",
                },
                {
                    role: "assistant",
                    content: null,
                    tool_calls: [
                        {
                            id: "call_whzFC5Mlx17thYJYOvdWf7RZ",
                            type: "function",
                            function: {
                                name: "UnknownTool",
                                arguments: "{}",
                            },
                        },
                    ],
                    date: "2025-08-08T10:28:01.5884757Z",
                },
                {
                    tool_call_id: "call_whzFC5Mlx17thYJYOvdWf7RZ",
                    role: "tool",
                    content: '[{"Name":"Beverages","Description":"Soft drinks, coffees, teas, beers, and ales"}]',
                    date: "2025-08-08T10:28:02.5884757Z",
                },
                {
                    content: null,
                    role: "assistant",
                    tool_calls: [
                        {
                            function: {
                                arguments: '{"subAgentUserPrompt":"Explain how to query documents in RavenDB"}',
                                name: "raven-expert-agent",
                            },
                            id: "call_CscbrKZ4VC1GibM2oERi7nF9",
                            type: "function",
                        },
                    ],
                    date: "2025-08-08T10:28:33.5884757Z",
                },
                {
                    tool_call_id: "call_CscbrKZ4VC1GibM2oERi7nF9",
                    role: "tool",
                    content: "Sub agent answer",
                    subConversationId: "Chats/2",
                    date: "2025-08-08T10:28:34.5884757Z",
                },
                {
                    content: null,
                    role: "assistant",
                    tool_calls: [
                        {
                            function: {
                                arguments: '{"Query":["test"]}',
                                name: "ActionProductSearch",
                            },
                            id: "call_MdKvWaFtl0cJAc5a0q26Lo99",
                            type: "function",
                        },
                    ],
                    date: "2025-08-08T10:28:35.5884757Z",
                },
            ],
            LinkedConversations: [],
            TotalUsage: {
                PromptTokens: 1926,
                CompletionTokens: 679,
                TotalTokens: 2625,
                CachedTokens: 0,
                ReasoningTokens: 20,
            },
            OpenActionCalls: {
                call_MdKvWaFtl0cJAc5a0q26Lo97: {
                    Name: "ActionProductSearch",
                    ToolId: "call_MdKvWaFtl0cJAc5a0q26Lo99",
                    Arguments: "{}",
                    Type: "UserAction",
                    SubConversationId: null,
                },
            },
            LastMessageAt: "2025-08-08T10:28:30.5884757Z",
            CreatedAt: "2025-08-08T10:28:20.5582091Z",
            Expires: null,
            "@metadata": {
                "@collection": "@conversations",
                "@change-vector": "A:9023-zbpmCj4aA0WLV/H8tjaiag",
                "@id": "Chats/0000000000000009018-A",
                "@last-modified": "2025-08-20T09:27:14.9226011Z",
            },
        });
    }
}
