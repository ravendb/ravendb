/// <reference path="../../../../typings/tsd.d.ts" />

class inProgressAnimator implements disposable {

    private inProgressArea: number[][] = [];
    private canvas: HTMLCanvasElement;
    private context: CanvasRenderingContext2D;

    private inMemoryStripesCanvas: HTMLCanvasElement;

    private hasTimerRunning = false;
    private disposed = false;

    private static readonly stripesPadding = 10;

    constructor(canvas: HTMLCanvasElement) {
        this.canvas = canvas;
        this.context = canvas.getContext("2d");
    }

    private fillWithStripes(color: string) {
        const context = this.inMemoryStripesCanvas.getContext("2d");

        context.strokeStyle = color;
        context.lineWidth = 3;

        const height = this.inMemoryStripesCanvas.height;
        const widthAndHeight = this.inMemoryStripesCanvas.width + height;

        for (let dx = -widthAndHeight; dx <= widthAndHeight; dx += inProgressAnimator.stripesPadding) {
            context.moveTo(dx - height, height);
            context.lineTo(dx, 0);
        }
        context.stroke();
    }

    reset() {
        this.inProgressArea = [];
    }

    register(input: number[]) {
        this.inProgressArea.push(input);
    }

    dispose() {
        this.disposed = true;
    }

    animate(color: string) {
        if (this.hasTimerRunning) {
            return;
        }
        
        this.init(color);

        if (this.inProgressArea.length) {
            this.hasTimerRunning = true;

            d3.timer(() => this.onFrame());
        } else {
            this.clear();
        }
    }
    
    private init(color: string) {
        this.inMemoryStripesCanvas = document.createElement("canvas");
        this.inMemoryStripesCanvas.width = this.canvas.width + 2 * inProgressAnimator.stripesPadding;
        this.inMemoryStripesCanvas.height = this.canvas.height;

        this.fillWithStripes(color);
    }

    private clear() {
        const context = this.context;
        context.clearRect(0, 0, this.canvas.width, this.canvas.height);
    }

    private onFrame() {
        if (this.disposed || this.inProgressArea.length === 0) {
            this.clear();
            this.hasTimerRunning = false;
            return true;
        } 

        this.clear();
        this.draw();

        return false;
    }

    private draw() {

        const animationDuration = 600;
        const progress = (new Date().getTime() % animationDuration) / animationDuration;
        const currentShift = Math.floor(progress * inProgressAnimator.stripesPadding);

        const context = this.context;

        context.clearRect(0, 0, this.canvas.width, this.canvas.height);

        context.save();
        try {
            context.beginPath();
            this.inProgressArea.forEach(area => {
                context.rect(area[0], area[1], area[2], area[3]);
            });
            
            context.clip();

            context.drawImage(this.inMemoryStripesCanvas, -currentShift, 0);

        } finally {
            this.context.restore();
        }
    }

  
}

export = inProgressAnimator;
