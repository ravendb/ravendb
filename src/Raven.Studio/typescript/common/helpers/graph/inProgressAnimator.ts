/// <reference path="../../../../typings/tsd.d.ts" />

class inProgressAnimator {

    private inProgressArea: number[][] = [];
    private canvas: HTMLCanvasElement;
    private context: CanvasRenderingContext2D;

    private hasTimerRunning = false;
    private startTime: number;

    constructor(canvas: HTMLCanvasElement) {
        this.canvas = canvas;
        this.context = canvas.getContext("2d");
    }

    reset() {
        this.inProgressArea = [];
    }

    register(input: number[]) {
        this.inProgressArea.push(input);
    }

    animate() {
        if (this.hasTimerRunning) {
            return;
        }

        if (this.inProgressArea.length) {
            this.hasTimerRunning = true;

            this.startTime = new Date().getTime();

            d3.timer(() => this.onFrame());
        } else {
            this.clear();
        }
    }

    private clear() {
        const context = this.context;
        context.clearRect(0, 0, this.canvas.width, this.canvas.height);
    }

    private onFrame() {
        if (this.inProgressArea.length === 0) {
            this.hasTimerRunning = false;
            return true;
        } 

        this.clear();
        this.draw();

        return false;
    }

    private draw() {
        const context = this.context;
        context.save();
        try {
            context.beginPath();
            this.inProgressArea.forEach(area => {
                context.rect(area[0], area[1], area[2], area[3]);
            });
            
            context.clip();
            context.globalAlpha = 0.2 * this.alfaDiff();

            context.fill();
        } finally {
            this.context.restore();
        }
    }

    private alfaDiff(): number {
        const timeDelta = new Date().getTime() - this.startTime;
        const animationTime = 1500;
        const halfTime = animationTime / 2;
        const animationPosition = timeDelta % animationTime;

        if (animationPosition < halfTime) {
            // animate forward
            return animationPosition * 1.0 / halfTime;
        } else {
            // animate backwards
            return 1.0 - 1.0 * (animationPosition - halfTime) / halfTime;
        }
    }

  
}

export = inProgressAnimator;