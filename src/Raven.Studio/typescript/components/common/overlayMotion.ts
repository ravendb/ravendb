import "./OverlayMotion.scss";
import { OverlayProps } from "react-bootstrap/Overlay";

export const overlayMotionClassName = "overlay-motion";
export const overlayPendingClassName = "overlay-pending";

const topLeftPositioning = { name: "computeStyles", options: { gpuAcceleration: false } };

export function withTopLeftPositioning(popperConfig: OverlayProps["popperConfig"] = {}): OverlayProps["popperConfig"] {
    return { ...popperConfig, modifiers: [topLeftPositioning, ...(popperConfig.modifiers ?? [])] };
}
