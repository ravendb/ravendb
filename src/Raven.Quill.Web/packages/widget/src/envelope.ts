/** The wire shape shared by every postMessage this widget sends or accepts, in both directions and in
 *  both modes. Mirrored in C# by `Raven.Quill.Embed.HostChannel`, which the notice shells post from. */
export const ENVELOPE_SOURCE = "raven-quill";
export const ENVELOPE_VERSION = 1;

export type Envelope<TType extends string, TPayload> = {
    source: typeof ENVELOPE_SOURCE;
    version: typeof ENVELOPE_VERSION;
    type: TType;
    payload: TPayload;
};

export function envelope<TType extends string, TPayload>(type: TType, payload: TPayload): Envelope<TType, TPayload> {
    return { source: ENVELOPE_SOURCE, version: ENVELOPE_VERSION, type, payload };
}

export function isEnvelope(value: unknown): value is Envelope<string, unknown> {
    if (typeof value !== "object" || value === null) return false;
    const candidate = value as Record<string, unknown>;
    return (
        candidate.source === ENVELOPE_SOURCE &&
        candidate.version === ENVELOPE_VERSION &&
        typeof candidate.type === "string"
    );
}
