import "./IconAscii.scss";
import { useEffect, useRef } from "react";
import { useThemeColors } from "./useThemeColors";
import { useParticles } from "./useParticles";
import { settings, shapeTemplate } from "./config";

export default function IconAsciiPlaceholder() {
    const canvasRef = useRef<HTMLCanvasElement | null>(null);
    const particles = useParticles(shapeTemplate, settings);
    const { baseColor } = useThemeColors();

    useEffect(() => {
        const canvas = canvasRef.current;
        if (!canvas) {
            return;
        }

        const ctx = canvas.getContext("2d");
        if (!ctx) {
            return;
        }
        const dpr = window.devicePixelRatio || 1;

        canvas.width = settings.boxSize * dpr;
        canvas.height = settings.boxSize * dpr;
        canvas.style.width = `${settings.boxSize}px`;
        canvas.style.height = `${settings.boxSize}px`;
        canvas.style.cursor = "default";
        ctx.scale(dpr, dpr);
        ctx.clearRect(0, 0, settings.boxSize, settings.boxSize);
        ctx.font = settings.font;
        ctx.textBaseline = "top";
        ctx.fillStyle = baseColor.current;

        particles.forEach((p) => {
            ctx.fillText(p.original, p.x, p.y);
        });
    }, [baseColor, particles]);

    return (
        <div className="ascii-wrapper">
            <canvas ref={canvasRef} />
        </div>
    );
}
