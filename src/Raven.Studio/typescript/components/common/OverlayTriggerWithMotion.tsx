import classNames from "classnames";
import { overlayMotionClassName, withTopLeftPositioning } from "components/common/overlayMotion";
import { cloneElement, isValidElement, ReactNode } from "react";
import { OverlayChildren } from "react-bootstrap/Overlay";
import OverlayTrigger, { OverlayTriggerProps } from "react-bootstrap/OverlayTrigger";

export default function OverlayTriggerWithMotion({ overlay, popperConfig, ...props }: OverlayTriggerProps) {
    return (
        <OverlayTrigger {...props} overlay={withMotion(overlay)} popperConfig={withTopLeftPositioning(popperConfig)} />
    );
}

function withMotion(overlay: OverlayChildren): OverlayChildren {
    if (typeof overlay === "function") {
        return (injectedProps) => withMotionClass(overlay(injectedProps));
    }

    return withMotionClass(overlay);
}

function withMotionClass<T extends ReactNode>(node: T): T {
    if (!isValidElement<{ className?: string }>(node)) {
        return node;
    }

    return cloneElement(node, { className: classNames(node.props.className, overlayMotionClassName) }) as T;
}
