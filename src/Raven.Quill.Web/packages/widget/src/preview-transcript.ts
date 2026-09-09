import type { HistoryTurn } from "@/widget-config";

/** The canned transcript the dashboard's styling preview renders. Deliberately exercises the parts an
 *  operator needs to judge a theme by: a user bubble, prose, a GFM table and a fenced code block. */
export const PREVIEW_TRANSCRIPT: HistoryTurn[] = [
    { role: "user", content: "Which plans include priority support?" },
    {
        role: "assistant",
        content: `Two of the three do. Here's how they compare:

| Plan | Priority support | Seats |
| --- | --- | --- |
| Starter | No | 3 |
| Team | Yes | 25 |
| Enterprise | Yes | Unlimited |

You can check the current plan from the API:

\`\`\`bash
curl -s https://api.example.com/v1/subscription
\`\`\`

Let me know if you'd like the upgrade steps.`,
    },
];
