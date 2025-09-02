import genUtils from "common/generalUtils";

interface AiTokensUsagePopoverBodyProps {
    prompt: number;
    completion: number;
    cached: number;
    total: number;
}

export default function AiTokensUsagePopoverBody({ prompt, completion, cached, total }: AiTokensUsagePopoverBodyProps) {
    return (
        <div>
            <div className="hstack justify-content-between gap-3">
                <span>Prompt tokens</span>
                <span>{genUtils.formatAiTokens(prompt)}</span>
            </div>
            <div className="hstack justify-content-between gap-3">
                <span>Completion tokens</span>
                <span>{genUtils.formatAiTokens(completion)}</span>
            </div>
            <div className="hstack justify-content-between gap-3">
                <span>Cached tokens</span>
                <span>{genUtils.formatAiTokens(cached)}</span>
            </div>
            <hr className="my-1" />
            <div className="hstack justify-content-between gap-3">
                <span>Total tokens used</span>
                <span>{genUtils.formatAiTokens(total)}</span>
            </div>
        </div>
    );
}
