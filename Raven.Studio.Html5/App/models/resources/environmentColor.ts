class environmentColor {
    public name: string;
    public backgroundColor: string;
    public textColor: string;

    constructor(name: string, backgroundColor: string, textColor: string = null) {
        this.name = name;
        this.backgroundColor = backgroundColor;
        this.textColor = !!textColor ? textColor : backgroundColor;
    }

    toDto() :  environmentColorDto {
        return {
            Name: this.name,
            BackgroundColor: this.backgroundColor,
            TextColor: this.textColor
        };
    }
} 

export = environmentColor;
