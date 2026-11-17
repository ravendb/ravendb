import "./OverlayMotion.scss";
import { OverlayProps } from "react-bootstrap/Overlay";

export const OVERLAY_MOTION_CLASS_NAME = "overlay-motion";

const TOP_LEFT_POSITIONING = { name: "computeStyles", options: { gpuAcceleration: false } };

export function withTopLeftPositioning(popperConfig: OverlayProps["popperConfig"] = {}): OverlayProps["popperConfig"] {
    return { ...popperConfig, modifiers: [TOP_LEFT_POSITIONING, ...(popperConfig.modifiers ?? [])] };
}
