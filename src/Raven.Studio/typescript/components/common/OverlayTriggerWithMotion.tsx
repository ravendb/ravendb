import classNames from "classnames";
import { OVERLAY_MOTION_CLASS_NAME, withTopLeftPositioning } from "components/common/overlayMotion";
import { cloneElement, isValidElement, ReactElement } from "react";
import { OverlayChildren, OverlayInjectedProps } from "react-bootstrap/Overlay";
// eslint-disable-next-line no-restricted-imports
import OverlayTrigger, { OverlayTriggerProps } from "react-bootstrap/OverlayTrigger";

export default function OverlayTriggerWithMotion({ overlay, popperConfig, ...props }: OverlayTriggerProps) {
    return (
        <OverlayTrigger {...props} overlay={withMotion(overlay)} popperConfig={withTopLeftPositioning(popperConfig)} />
    );
}

function withMotion(overlay: OverlayChildren): OverlayChildren {
    if (typeof overlay === "function") {
        return (injectedProps) => {
            const node = overlay(injectedProps);
            return isValidElement<OverlayInjectedProps>(node) ? withMotionClass(node) : node;
        };
    }

    return withMotionClass(overlay);
}

function withMotionClass(element: ReactElement<OverlayInjectedProps>) {
    return cloneElement(element, { className: classNames(element.props.className, OVERLAY_MOTION_CLASS_NAME) });
}
