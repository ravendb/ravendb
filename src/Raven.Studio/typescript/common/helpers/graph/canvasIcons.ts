/// <reference path="../../../../typings/tsd.d.ts" />

/**
 * Svg -> canvas converted using: http://demo.qunee.com/svg2canvas/
 */
class canvasIcons {

    static cancel(ctx: CanvasRenderingContext2D, x: number, y: number, width: number) {
        ctx.save();

        const scaleFactor = width * 1.0 / 64;
        ctx.translate(x, y);
        ctx.scale(scaleFactor, scaleFactor);

        ctx.beginPath();
        ctx.moveTo(39.9, 32);
        ctx.lineTo(54.7, 17.3);
        ctx.lineTo(46.7, 9.4);
        ctx.lineTo(32, 24.1);
        ctx.lineTo(17.3, 9.3);
        ctx.lineTo(9.3, 17.3);
        ctx.lineTo(24.1, 32);
        ctx.lineTo(9.4, 46.7);
        ctx.lineTo(17.3, 54.6);
        ctx.lineTo(32, 39.9);
        ctx.lineTo(46.7, 54.7);
        ctx.lineTo(54.7, 46.7);
        ctx.lineTo(39.9, 32);
        ctx.closePath();
        ctx.fill();
        ctx.stroke();

        ctx.restore();
    }

    static trash(ctx: CanvasRenderingContext2D, x: number, y: number, width: number) {
       ctx.save();

       const scaleFactor = width * 1.0 / 64;
       ctx.translate(x, y);
       ctx.scale(scaleFactor, scaleFactor);

       ctx.beginPath();
       ctx.moveTo(51.4, 9.8);
       ctx.lineTo(51.4, 15.4); 
       ctx.lineTo(12.6, 15.4);
       ctx.lineTo(12.6, 9.8);
       ctx.lineTo(22.299999999999997, 9.8);
       ctx.lineTo(25, 7.1);
       ctx.lineTo(38.9, 7.1);
       ctx.lineTo(41.6, 9.8);
       ctx.lineTo(51.4, 9.8);
       ctx.closePath();
       ctx.moveTo(15.4, 51.3);
       ctx.lineTo(15.4, 18.1);
       ctx.lineTo(48.6, 18.1);
       ctx.lineTo(48.6, 51.300000000000004);
       ctx.bezierCurveTo(48.6, 52.900000000000006, 48.1, 54.2, 47, 55.300000000000004);
       ctx.bezierCurveTo(45.9, 56.400000000000006, 44.6, 56.900000000000006, 43.2, 56.900000000000006);
       ctx.lineTo(20.9, 56.900000000000006);
       ctx.bezierCurveTo(19.299999999999997, 56.900000000000006, 18, 56.400000000000006, 16.9, 55.300000000000004);
       ctx.bezierCurveTo(15.9, 54.2, 15.4, 52.9, 15.4, 51.3);
       ctx.closePath();
       ctx.fill();
       ctx.stroke();

       ctx.restore();
   }
}

export = canvasIcons;
