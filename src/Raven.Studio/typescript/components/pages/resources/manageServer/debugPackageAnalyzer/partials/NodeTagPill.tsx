import React from "react";
import colorsManager from "common/colorsManager";

interface NodeTagPillProps {
    tag: string;
}

// Circular node avatar matching the cluster dashboard node icons (.node-label): a compact colored
// circle with the node letter. Colored from the shared palette (colorsManager.nodeColors), indexed
// by tag so each node is visually distinct and consistent with the rest of the Studio. The letter
// color (--well-bg, the panel background) is set in scss so it reads as a cut-out, like .node-label.
export default function NodeTagPill({ tag }: NodeTagPillProps) {
    return (
        <span className="node-tag-circle" style={{ backgroundColor: nodeColor(tag) }} title={`Node ${tag}`}>
            {tag}
        </span>
    );
}

function nodeColor(tag: string): string {
    const colors = colorsManager.nodeColors;
    if (!tag) {
        return colors[0];
    }
    // node tags are letters (A, B, C, ...); map A -> first palette color to match the cluster dashboard
    const index = tag.length === 1 ? tag.toUpperCase().charCodeAt(0) - 65 : sumCharCodes(tag);
    const safeIndex = ((index % colors.length) + colors.length) % colors.length;
    return colors[safeIndex];
}

function sumCharCodes(value: string): number {
    let sum = 0;
    for (let i = 0; i < value.length; i++) {
        sum += value.charCodeAt(i);
    }
    return sum;
}
