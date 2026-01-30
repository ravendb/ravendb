import { Meta } from "@storybook/react-webpack5";
import Code from "./Code";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";

export default {
    title: "Common/Code",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

export function LanguageVariants() {
    return (
        <div className="vstack gap-4 mb-4">
            <h2>Language Variants</h2>

            <div>
                <h3>JavaScript</h3>
                <Code
                    code={sampleCode.javascript}
                    language="javascript"
                    elementToCopy={sampleCode.javascript}
                    className="border border-secondary rounded"
                />
            </div>

            <div>
                <h3>C#</h3>
                <Code
                    className="border border-secondary rounded"
                    code={sampleCode.csharp}
                    language="csharp"
                    elementToCopy={sampleCode.csharp}
                />
            </div>

            <div>
                <h3>JSON</h3>
                <Code
                    code={sampleCode.json}
                    language="json"
                    elementToCopy={sampleCode.json}
                    className="border border-secondary rounded"
                />
            </div>

            <div>
                <h3>HTML</h3>
                <Code
                    code={sampleCode.html}
                    language="html"
                    elementToCopy={sampleCode.html}
                    className="border border-secondary rounded"
                />
            </div>

            <div>
                <h3>CSS</h3>
                <Code
                    code={sampleCode.css}
                    language="css"
                    elementToCopy={sampleCode.css}
                    className="border border-secondary rounded"
                />
            </div>
        </div>
    );
}

export function WithAndWithoutCopyButton() {
    return (
        <div className="vstack gap-4">
            <h2>Copy Button Variants</h2>

            <div>
                <h3>With Copy Button</h3>
                <Code
                    code={sampleCode.javascript}
                    language="javascript"
                    elementToCopy={sampleCode.javascript}
                    className="border border-secondary rounded"
                />
            </div>

            <div>
                <h3>Without Copy Button</h3>
                <Code code={sampleCode.javascript} language="javascript" className="border border-secondary rounded" />
            </div>
        </div>
    );
}

export function WithCustomClassName() {
    return (
        <div className="vstack gap-4">
            <h2>Custom Class Names</h2>

            <div>
                <h3>Default</h3>
                <Code
                    code={sampleCode.json}
                    language="json"
                    elementToCopy={sampleCode.json}
                    className="border border-secondary rounded"
                />
            </div>

            <div>
                <h3>With Custom Class</h3>
                <Code
                    code={sampleCode.json}
                    language="json"
                    elementToCopy={sampleCode.json}
                    className="border border-primary rounded"
                />
            </div>
        </div>
    );
}

export function PlainTextExample() {
    return (
        <div className="vstack gap-4">
            <h2>Plain Text</h2>
            <Code
                code={sampleCode.plainText}
                language="plaintext"
                elementToCopy={sampleCode.plainText}
                className="border border-secondary rounded"
            />
        </div>
    );
}

export function LargeCodeExample() {
    return (
        <div className="vstack gap-4">
            <h2>Large Code Example</h2>
            <Code
                code={sampleCode.largeCode.trim()}
                language="javascript"
                elementToCopy={sampleCode.largeCode.trim()}
                className="border border-secondary rounded"
            />
        </div>
    );
}

const sampleCode = {
    javascript: `function greetUser(name) {
    console.log(\`Hello, \${name}!\`);
    return \`Welcome, \${name}\`;
}

const user = "John Doe";
greetUser(user);`,

    csharp: `using System;
using System.Collections.Generic;

public class Program
{
    public static void Main()
    {
        var numbers = new List<int> { 1, 2, 3, 4, 5 };
        var sum = numbers.Sum();
        Console.WriteLine($"Sum: {sum}");
    }
}`,

    json: `{
    "name": "RavenDB",
    "version": "6.2.0",
    "description": "ACID Document Database",
    "features": [
        "ACID Transactions",
        "Cluster-Wide Transactions",
        "Document Store",
        "Full-Text Search"
    ],
    "license": "MIT"
}`,

    html: `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>RavenDB Studio</title>
</head>
<body>
    <div class="container">
        <h1>Welcome to RavenDB</h1>
        <p>ACID Document Database</p>
    </div>
</body>
</html>`,

    css: `.code-container {
    position: relative;
    display: flex;
    flex-direction: column;
    background: #1e1e1e;
    border-radius: 8px;
    overflow: hidden;
}

.code-header {
    background: #2d2d30;
    padding: 8px 16px;
    border-bottom: 1px solid #3e3e42;
}

.code-content {
    flex: 1;
    overflow: auto;
}`,

    largeCode: `
// Large code example demonstrating scrolling
function generateData(count) {
    const data = [];
    for (let i = 0; i < count; i++) {
        data.push({
            id: i,
            name: \`Item \${i}\`,
            value: Math.random() * 100,
            timestamp: new Date().toISOString(),
            metadata: {
                category: \`Category-\${i % 5}\`,
                priority: Math.floor(Math.random() * 10),
                tags: [\`tag\${i}\`, \`item\${i}\`, \`data\${i}\`]
            }
        });
    }
    return data;
}

function processData(data) {
    return data
        .filter(item => item.value > 50)
        .map(item => ({
            ...item,
            processed: true,
            processingTime: Date.now()
        }))
        .sort((a, b) => b.value - a.value);
}

// This would be a much longer file in practice
// with many more functions and examples
// demonstrating how the Code component handles
// large amounts of content with scrolling
    `,

    plainText: `This is a plain text example.
It doesn't have syntax highlighting.
Useful for logs, configuration files, or any text content.

Line 1: Some information
Line 2: More details
Line 3: Final notes`,
};
