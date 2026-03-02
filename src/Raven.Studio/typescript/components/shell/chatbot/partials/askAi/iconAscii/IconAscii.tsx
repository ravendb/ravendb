import "./IconAscii.scss";
import { useEffect, useRef } from "react";
import { useThemeColors } from "./useThemeColors";
import { useMouseTracking } from "./useMouseTracking";
import { useParticles } from "./useParticles";
import { lerpColor } from "./helpers";
import { settings, shapeTemplate } from "./config";
import { chatbotActions, chatbotSelectors } from "components/shell/chatbot/store/chatbotSlice";
import { useAppDispatch, useAppSelector } from "components/store";

export default function AsciiLogo() {
    const dispatch = useAppDispatch();
    const isAnimationFinished = useAppSelector(chatbotSelectors.isAsciiAnimationFinished);

    const canvasRef = useRef<HTMLCanvasElement | null>(null);
    const mouseRef = useMouseTracking();
    const particles = useParticles(shapeTemplate, settings);
    const { activeColor, baseColor } = useThemeColors();

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
        ctx.scale(dpr, dpr);

        let animationId = 0;
        let frameCount = 0;
        const scanEndIndex = particles.length + settings.scanTailPadding;
        let currentScanIndex = isAnimationFinished ? scanEndIndex : 0;
        const hoverRadiusSq = settings.hoverRadius * settings.hoverRadius;
        let time = 0;
        let introFinished = isAnimationFinished;

        const render = () => {
            ctx.clearRect(0, 0, settings.boxSize, settings.boxSize);
            time += settings.gradientSpeed;

            const isScanning = !introFinished && frameCount > settings.startDelay;
            if (isScanning && currentScanIndex < scanEndIndex) {
                currentScanIndex += settings.charsPerFrame;
            }

            ctx.font = settings.font;
            ctx.textBaseline = "top";

            const rect = canvas.getBoundingClientRect();
            const mx = mouseRef.current.x - rect.left;
            const my = mouseRef.current.y - rect.top;

            let hasActiveTimers = false;

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
                    hasActiveTimers = true;
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

            const isScanFinished = currentScanIndex >= scanEndIndex;
            if (!introFinished && isScanFinished && !hasActiveTimers) {
                introFinished = true;
                dispatch(chatbotActions.asciiAnimationFinished());
            }

            frameCount++;
            animationId = requestAnimationFrame(render);
        };

        render();

        return () => {
            cancelAnimationFrame(animationId);
        };
    }, [mouseRef, activeColor, baseColor, isAnimationFinished]);

    return (
        <div className="ascii-wrapper">
            <canvas ref={canvasRef} />
        </div>
    );
}
