import { Particle } from "./types";

export const useParticles = (shapeTemplate: string, settings: any): Particle[] => {
    const particles: Particle[] = [];
    const lines = shapeTemplate.split("\n");
    const totalRows = lines.length;
    const maxCols = Math.max(...lines.map((line) => line.length));

    const startX = (settings.boxSize - maxCols * settings.charSize) / 2;
    const startY = (settings.boxSize - totalRows * settings.lineHeight) / 2;

    lines.forEach((line, row) => {
        line.split("").forEach((char, col) => {
            if (char.trim() !== "") {
                particles.push({
                    x: startX + col * settings.charSize,
                    y: startY + row * settings.lineHeight,
                    w: settings.charSize,
                    h: settings.lineHeight,
                    row,
                    col,
                    original: char,
                    current: char,
                    timer: 0,
                    centerX: startX + col * settings.charSize + settings.charSize / 2,
                    centerY: startY + row * settings.lineHeight + settings.lineHeight / 2,
                });
            }
        });
    });

    return particles;
};
