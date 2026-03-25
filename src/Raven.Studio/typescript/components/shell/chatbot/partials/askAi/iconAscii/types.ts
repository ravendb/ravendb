export interface Color {
    r: number;
    g: number;
    b: number;
}

export interface Particle {
    x: number;
    y: number;
    w: number;
    h: number;
    row: number;
    col: number;
    original: string;
    current: string;
    timer: number;
    centerX: number;
    centerY: number;
}
