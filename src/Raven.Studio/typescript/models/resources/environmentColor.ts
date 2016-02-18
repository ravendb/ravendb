/// <reference path="../../../typings/tsd.d.ts"/>

class environmentColor {
    public name: string;
    public backgroundColor: string;
    public textColor: string;

    constructor(name: string, backgroundColor: string) {
        this.name = name;
        this.backgroundColor = backgroundColor;
    }

    toDto() :  environmentColorDto {
        return {
            Name: this.name,
            BackgroundColor: this.backgroundColor
        };
    }
} 

export = environmentColor;
