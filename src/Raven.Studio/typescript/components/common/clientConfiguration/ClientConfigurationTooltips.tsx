export function IdentityPartsSeparatorTooltip() {
    return (
        <>
            Set the default separator for automatically generated document IDs (<i>Identity</i>, <i>HiLo</i>, and{" "}
            <i>Server-side</i>).
            <br />
            Use any character except <code>&apos;|&apos;</code> (pipe).
        </>
    );
}

export function MaximumNumberOfRequestsTooltip() {
    return (
        <>
            Set this number to restrict the number of requests (<code>Reads</code> & <code>Writes</code>) per session in
            the client API.
        </>
    );
}

export function LoadBalanceBehaviorTooltip() {
    return (
        <>
            <span className="d-inline-block mb-1">
                Set the Load balance method for <strong>Read</strong> & <strong>Write</strong> requests.
            </span>
            <ul>
                <li className="mb-1">
                    <code>None</code>
                    <br />
                    <strong>Read</strong> requests - the node the client will target will be based on Read balance
                    behavior configuration.
                    <br />
                    <strong>Write</strong> requests - will be sent to the preferred node.
                </li>
                <li className="mb-1">
                    <code>Use session context</code>
                    <br />
                    Sessions that are assigned the same context will have all their <strong>Read & Write</strong>{" "}
                    requests routed to the same node.
                    <br />
                    The session context is hashed from a context string (given by the client) and an optional seed.
                </li>
            </ul>
        </>
    );
}

export function LoadBalancerSeedTooltip() {
    return (
        <>
            An optional seed number.
            <br />
            Used when hashing the session context.
        </>
    );
}

export function ReadBalanceBehaviorTooltip() {
    return (
        <>
            Set the Read balance method the client will use when accessing a node with <code>Read</code> requests.
            <br />
            <code>Write</code> requests are sent to the preferred node.
        </>
    );
}
