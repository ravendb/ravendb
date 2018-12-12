import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class chooseTheme extends dialogViewModelBase {
    
    static readonly supportedThemes = ["blue", "dark", "light"];
    static readonly themeLocalStorageKey = "raven-theme";
    static readonly stylesheetPrefix = "Content/css/";
    
    static readonly defaultTheme = "dark";
    
    static readonly themeToStylesheet = {
        "dark" : "styles.css",
        "light": "styles-light.css",
        "blue": "styles-blue.css"
    } as dictionary<string>;
    
    currentTheme = ko.observable<string>();
    
    constructor() {
        super();
        const savedTheme = localStorage.getItem(chooseTheme.themeLocalStorageKey);
        const currentTheme = savedTheme && chooseTheme.supportedThemes.indexOf(savedTheme) !== -1
            ? savedTheme
            : chooseTheme.defaultTheme;
        
        this.currentTheme(currentTheme);
    }
    
    useTheme(theme: string) {
        localStorage.setItem(chooseTheme.themeLocalStorageKey, theme);
        this.updateStylesheet(chooseTheme.themeToStylesheet[theme]);
        this.close();
    }
    
    private updateStylesheet(fileName: string) {
        const themeLink = document.getElementById("raven-theme") as HTMLLinkElement;
        themeLink.href = chooseTheme.stylesheetPrefix + fileName;
    }
}

export = chooseTheme;
