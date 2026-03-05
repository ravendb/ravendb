import React, { useEffect, useRef, useState } from "react";
import "./IconAscii.scss";
import { useParticles } from "./useParticles";
import { settings, shapeTemplate } from "./config";

export default function IconAsciiPlaceholder() {
    const canvasRef = useRef<HTMLCanvasElement | null>(null);
    const particles = useParticles(shapeTemplate, settings);

    const [color, setColor] = useState<string>("#888888");

    useEffect(() => {
        const updateColor = () => {
            const bodyStyle = getComputedStyle(document.body);
            const rootStyle = getComputedStyle(document.documentElement);
            const newColor =
                bodyStyle.getPropertyValue("--border-color-light").trim() ||
                rootStyle.getPropertyValue("--border-color-light").trim() ||
                "#888888";

            setColor(newColor);
        };

        updateColor();

        const observer = new MutationObserver(updateColor);
        observer.observe(document.body, { attributes: true, attributeFilter: ["class", "data-theme"] });
        observer.observe(document.documentElement, { attributes: true, attributeFilter: ["class", "data-theme"] });

        return () => observer.disconnect();
    }, []);

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
        canvas.style.cursor = "default";
        ctx.scale(dpr, dpr);
        ctx.clearRect(0, 0, settings.boxSize, settings.boxSize);
        ctx.font = settings.font;
        ctx.textBaseline = "top";
        ctx.fillStyle = color;

        particles.forEach((p) => {
            ctx.fillText(p.original, p.x, p.y);
        });
    }, [color, particles]);

    return (
        <div className="ascii-wrapper">
            <canvas ref={canvasRef} />
        </div>
    );
}
