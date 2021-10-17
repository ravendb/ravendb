import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class chooseTheme extends dialogViewModelBase {

    view = require("views/shell/chooseTheme.html");
    
    static readonly supportedThemes = ["blue", "dark", "light"];
    static readonly themeLocalStorageKey = "raven-theme";
    static readonly stylesheetPrefix = "styles/";
    
    static readonly defaultTheme = "dark";
    
    static readonly themeToStylesheet: dictionary<string> = {
        "dark" : "styles.css",
        "light": "styles-light.css",
        "blue": "styles-blue.css"
    };
    
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
