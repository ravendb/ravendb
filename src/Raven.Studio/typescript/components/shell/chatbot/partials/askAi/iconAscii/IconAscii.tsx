import React, { useEffect, useRef } from "react";
import "./IconAscii.scss";
import { useThemeColors } from "./useThemeColors";
import { useMouseTracking } from "./useMouseTracking";
import { useParticles } from "./useParticles";
import { lerpColor } from "./helpers";
import { settings, shapeTemplate } from "./config";

export default function AsciiLogo() {
    const canvasRef = useRef<HTMLCanvasElement | null>(null);
    const mouseRef = useMouseTracking();
    const particles = useParticles(shapeTemplate, settings);
    const { activeColor, baseColor } = useThemeColors();

    useEffect(() => {
        const canvas = canvasRef.current;
        if (!canvas) return;

        const ctx = canvas.getContext("2d");
        if (!ctx) return;
        const dpr = window.devicePixelRatio || 1;

        canvas.width = settings.boxSize * dpr;
        canvas.height = settings.boxSize * dpr;
        canvas.style.width = `${settings.boxSize}px`;
        canvas.style.height = `${settings.boxSize}px`;
        ctx.scale(dpr, dpr);

        let animationId: number;
        let frameCount = 0;
        let currentScanIndex = 0;
        const hoverRadiusSq = settings.hoverRadius * settings.hoverRadius;
        let time = 0;

        const render = () => {
            ctx.clearRect(0, 0, settings.boxSize, settings.boxSize);
            time += settings.gradientSpeed;

            const isScanning = frameCount > settings.startDelay;
            if (isScanning && currentScanIndex < particles.length + 50) {
                currentScanIndex += settings.charsPerFrame;
            }

            ctx.font = settings.font;
            ctx.textBaseline = "top";

            const rect = canvas.getBoundingClientRect();
            const mx = mouseRef.current.x - rect.left;
            const my = mouseRef.current.y - rect.top;

            particles.forEach((p, index) => {
                const dx = mx - p.centerX;
                const dy = my - p.centerY;
                const distSq = dx * dx + dy * dy;
                const isHovered = distSq < hoverRadiusSq;
                const isBeingScanned =
                    isScanning && index >= currentScanIndex && index < currentScanIndex + settings.charsPerFrame;

                if (isHovered || isBeingScanned) {
                    p.timer = settings.randomDuration;
                }

                if (p.timer > 0) {
                    p.timer--;
                    if (frameCount % settings.flickerSpeed === 0) {
                        p.current = settings.possibleChars.charAt(
                            Math.floor(Math.random() * settings.possibleChars.length)
                        );
                    }
                    ctx.fillStyle = activeColor.current;
                } else {
                    p.current = p.original;

                    if (index < currentScanIndex) {
                        const diagonalPos = p.x + p.y;
                        const wave = Math.sin(diagonalPos * settings.gradientScale - time);
                        const factor = (wave + 1) / 2;
                        ctx.fillStyle = lerpColor(settings.color1, settings.color2, factor);
                    } else {
                        ctx.fillStyle = baseColor.current;
                    }
                }

                ctx.fillText(p.current, p.x, p.y);
            });

            frameCount++;
            animationId = requestAnimationFrame(render);
        };

        render();

        return () => {
            cancelAnimationFrame(animationId);
        };
    }, [mouseRef, activeColor, baseColor]);

    return (
        <div className="ascii-wrapper">
            <canvas ref={canvasRef} />
        </div>
    );
}
