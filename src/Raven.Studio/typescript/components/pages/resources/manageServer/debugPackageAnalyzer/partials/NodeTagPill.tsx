import React from "react";
import colorsManager from "common/colorsManager";
import "./NodeTagPill.scss";

interface NodeTagPillProps {
    tag: string;
}

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
