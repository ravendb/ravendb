import { Color } from "./types";

export const settings = {
    boxSize: 200,
    font: "700 9px Menlo, Monaco, Consolas, 'Courier New', monospace",
    charSize: 5.2,
    lineHeight: 9,
    startDelay: 60,
    charsPerFrame: 2,
    scanTailPadding: 50,
    hoverRadius: 15,
    randomDuration: 10,
    flickerSpeed: 4,
    possibleChars: "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*",
    gradientSpeed: 0.025,
    gradientScale: 0.02,
    color1: { r: 56, g: 142, b: 233 } as Color,
    color2: { r: 123, g: 81, b: 255 } as Color,
};

export const shapeTemplate = `                                            
                                            
                                   ++       
                                  ++++      
    ++++++++++++++++++++++++++ ++++++++++   
   +++++++++++++++++++++++++++  ++++++++    
   ++++++++++++++++++++++++++++++ ++++      
   +++++++++++++++++ +++++++++++++ ++       
   ++++++++++++++++   ++++++++++++++++      
   +++++++++++++++    ++++++++++++++++      
   +++++++++++++         +++++++++++++      
   ++++++++++               ++++++++++      
   ++++++++++++++       ++++++++++++++      
   ++++++++++++++++   ++++++++++++++++      
   ++++++++++++++++   ++++++++++++++++      
   +++++++++++++++++ +++++++++++++++++      
   +++++++++++++++++++++++++++++++++++      
   +++++++++++++++++++++++++++++++++++      
   +++++++++++++++++++++++++++++++++        
   +++++++                                  
   +++++                                    
   +++                                      
                                            
                                            `;
